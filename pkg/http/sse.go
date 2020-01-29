package http

import (
	"context"
	"github.com/go-chi/render"
	"github.com/pkg/errors"
	"net/http"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
)

type StreamAdapter interface {
	// GetResponse returns the response to be sent back to client.
	// Any errors that occur should be handled and written to `w`, returning false as `ok`.
	GetResponse(w http.ResponseWriter, r *http.Request) (response interface{}, ok bool)
	// Validate validates if the incoming message should be handled by this handler.
	// Typically this involves checking some kind of model ID.
	Validate(r *http.Request, msg *message.Message) (ok bool)
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
	KeepAliveTimeout   time.Duration
}

func (c *SSERouterConfig) setDefaults() {
	if c.ErrorHandler == nil {
		c.ErrorHandler = DefaultErrorHandler
	}
}

func (c SSERouterConfig) validate() error {
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
	response, ok := h.streamAdapter.GetResponse(w, r)
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

	response, ok := h.streamAdapter.GetResponse(w, r)
	if !ok {
		return
	}

	responsesChan := make(chan interface{})

	go func() {
		defer func() {
			h.logger.Trace("Closing SSE handler", nil)
			close(responsesChan)
		}()

		responsesChan <- response

		h.logger.Trace("Listening for messages", nil)

		select {
		case msg := <-messages:
			msg.Ack()

			response, ok := h.processMessage(w, r, msg)
			if ok {
				responsesChan <- response
			}
		case <-time.After(h.config.KeepAliveTimeout):
			responsesChan <- render.EventStreamKeepAlive
		}

		select {
		case <-r.Context().Done():
			return
		default:
		}
	}()

	render.Respond(w, r, responsesChan)
}

func (h sseHandler) processMessage(w http.ResponseWriter, r *http.Request, msg *message.Message) (interface{}, bool) {
	ok := h.streamAdapter.Validate(r, msg)
	if !ok {
		return nil, false
	}

	h.logger.Trace("Received valid message", watermill.LogFields{"uuid": msg.UUID})

	return h.streamAdapter.GetResponse(w, r)
}
