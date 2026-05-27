package http

import (
	"context"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/render"
	"github.com/pkg/errors"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
)

type StreamAdapter interface {
	// InitialStreamResponse returns the first stream response to be sent back to client.
	// Any errors that occur should be handled and written to `w`.
	// Returning `ok` equal false ends processing the HTTP request.
	InitialStreamResponse(w http.ResponseWriter, r *http.Request) (response interface{}, ok bool)
	// NextStreamResponse returns the next stream response to be sent back to the client.
	// Typically this involves checking some kind of model ID extracted from the `msg`.
	// The response is sent to the client only if `ok` is true.
	// Any errors that occur should be either:
	//    1) logged and skipped, returning (nil, false)
	//    2) sent back to the client, returning (errorStruct, true)
	NextStreamResponse(r *http.Request, msg *message.Message) (response interface{}, ok bool)
}

type HandleErrorFunc func(w http.ResponseWriter, r *http.Request, err error)

type defaultErrorResponse struct {
	Error string `json:"error"`
}

// DefaultErrorHandler writes JSON error response along with Internal Server Error code (500).
func DefaultErrorHandler(w http.ResponseWriter, r *http.Request, err error) {
	w.WriteHeader(500)
	render.Respond(w, r, defaultErrorResponse{Error: err.Error()})
}

// SSERouter is a router handling Server-Sent Events.
type SSERouter struct {
	fanOut *gochannel.FanOut
	config SSERouterConfig
	logger watermill.LoggerAdapter
}

type SSERouterConfig struct {
	UpstreamSubscriber message.Subscriber
	ErrorHandler       HandleErrorFunc
	Marshaler          SSEMarshaler
}

func (c *SSERouterConfig) setDefaults() {
	if c.ErrorHandler == nil {
		c.ErrorHandler = DefaultErrorHandler
	}

	if c.Marshaler == nil {
		c.Marshaler = JSONSSEMarshaler{}
	}
}

func (c *SSERouterConfig) validate() error {
	if c.UpstreamSubscriber == nil {
		return errors.New("upstream subscriber is nil")
	}

	return nil
}

// NewSSERouter creates a new SSERouter.
func NewSSERouter(
	config SSERouterConfig,
	logger watermill.LoggerAdapter,
) (SSERouter, error) {
	config.setDefaults()
	if err := config.validate(); err != nil {
		return SSERouter{}, errors.Wrap(err, "invalid SSERouter config")
	}

	if logger == nil {
		logger = watermill.NopLogger{}
	}

	fanOut, err := gochannel.NewFanOut(config.UpstreamSubscriber, logger)
	if err != nil {
		return SSERouter{}, errors.Wrap(err, "could not create a FanOut")
	}

	return SSERouter{
		fanOut: fanOut,
		config: config,
		logger: logger,
	}, nil
}

// AddHandler starts a new handler for a given topic.
func (r SSERouter) AddHandler(topic string, streamAdapter StreamAdapter) http.HandlerFunc {
	r.logger.Trace("Adding handler for topic", watermill.LogFields{
		"topic": topic,
	})

	r.fanOut.AddSubscription(topic)

	handler := sseHandler{
		subscriber:    r.fanOut,
		topic:         topic,
		streamAdapter: streamAdapter,
		config:        r.config,
		logger:        r.logger,
	}

	return handler.Handle
}

// Run starts the SSERouter.
func (r SSERouter) Run(ctx context.Context) error {
	return r.fanOut.Run(ctx)
}

// Running is closed when the SSERouter is running.
func (r SSERouter) Running() chan struct{} {
	return r.fanOut.Running()
}

// Close stops the SSERouter.
func (r SSERouter) Close() error {
	return r.fanOut.Close()
}

type sseHandler struct {
	subscriber    message.Subscriber
	topic         string
	streamAdapter StreamAdapter
	config        SSERouterConfig
	logger        watermill.LoggerAdapter
}

func (h sseHandler) Handle(w http.ResponseWriter, r *http.Request) {
	if render.GetAcceptedContentType(r) == render.ContentTypeEventStream {
		h.handleEventStream(w, r)
		return
	}

	h.handleGenericRequest(w, r)
}

func (h sseHandler) handleGenericRequest(w http.ResponseWriter, r *http.Request) {
	response, ok := h.streamAdapter.InitialStreamResponse(w, r)
	if !ok {
		return
	}

	render.Respond(w, r, response)
}

func (h sseHandler) handleEventStream(w http.ResponseWriter, r *http.Request) {
	messages, err := h.subscriber.Subscribe(r.Context(), h.topic)
	if err != nil {
		h.config.ErrorHandler(w, r, err)
		return
	}

	response, ok := h.streamAdapter.InitialStreamResponse(w, r)
	if !ok {
		return
	}

	// chi pools its *Context and resets it after ServeHTTP returns. Our
	// goroutine outlives the handler in some paths (e.g. responder exiting
	// on ctx.Done()), so a later request can race the goroutine's
	// chi.URLParam calls on the reset Context. Detach a copy now.
	streamReq := requestWithDetachedChiContext(r)

	responsesChan := make(chan interface{})

	go func() {
		defer func() {
			h.logger.Trace("Closing SSE handler", nil)
			close(responsesChan)
		}()

		responsesChan <- response

		h.logger.Trace("Listening for messages", nil)

		for {
			select {
			case msg, ok := <-messages:
				if !ok {
					return
				}

				msg.Ack()

				nextResponse, ok := h.streamAdapter.NextStreamResponse(streamReq, msg)

				select {
				case <-r.Context().Done():
					return
				default:
				}

				if ok {
					h.logger.Trace("Stream responding on message", watermill.LogFields{"uuid": msg.UUID})
					responsesChan <- nextResponse
				}
			case <-r.Context().Done():
				return
			}
		}
	}()

	responder := sseResponder{
		marshaler: h.config.Marshaler,
	}
	responder.Respond(w, r, responsesChan)
}

// requestWithDetachedChiContext returns a copy of r whose context holds a
// fresh *chi.Context populated from the request's routing context. Used to
// safely access chi.URLParam from goroutines that may outlive ServeHTTP,
// because chi pools and resets *chi.Context after the handler returns.
func requestWithDetachedChiContext(r *http.Request) *http.Request {
	rctx := chi.RouteContext(r.Context())
	if rctx == nil {
		return r
	}

	copied := chi.NewRouteContext()
	copied.URLParams.Keys = append(copied.URLParams.Keys, rctx.URLParams.Keys...)
	copied.URLParams.Values = append(copied.URLParams.Values, rctx.URLParams.Values...)
	copied.RoutePatterns = append(copied.RoutePatterns, rctx.RoutePatterns...)
	copied.RoutePath = rctx.RoutePath
	copied.RouteMethod = rctx.RouteMethod
	copied.Routes = rctx.Routes

	return r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, copied))
}
