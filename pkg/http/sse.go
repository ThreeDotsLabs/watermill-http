package http

import (
	"net/http"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
	"github.com/go-chi/render"
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

type SSERouter struct {
	fanOut       gochannel.FanOut
	errorHandler HandleErrorFunc
	logger       watermill.LoggerAdapter
}

func NewSSERouter(
	upstreamRouter *message.Router,
	upstreamSubscriber message.Subscriber,
	errorHandler HandleErrorFunc,
	logger watermill.LoggerAdapter,
) (SSERouter, error) {
	if errorHandler == nil {
		errorHandler = DefaultErrorHandler
	}
	if logger == nil {
		logger = watermill.NopLogger{}
	}

	fanOut, err := gochannel.NewFanOut(upstreamRouter, upstreamSubscriber, logger)
	if err != nil {
		return SSERouter{}, err
	}

	return SSERouter{
		fanOut:       fanOut,
		errorHandler: errorHandler,
		logger:       logger,
	}, nil
}

func (r SSERouter) AddHandler(topic string, streamAdapter StreamAdapter) http.HandlerFunc {
	r.logger.Trace("Adding handler for topic", watermill.LogFields{
		"topic": topic,
	})

	r.fanOut.AddSubscription(topic)

	handler := sseHandler{
		subscriber:    r.fanOut,
		topic:         topic,
		streamAdapter: streamAdapter,
		errorHandler:  r.errorHandler,
		logger:        r.logger,
	}

	return handler.Handle
}

type sseHandler struct {
	subscriber    message.Subscriber
	topic         string
	streamAdapter StreamAdapter
	errorHandler  HandleErrorFunc
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
		h.errorHandler(w, r, err)
		return
	}

	responsesChan := make(chan interface{})

	go func() {
		defer func() {
			h.logger.Trace("Closing SSE handler", nil)
			close(responsesChan)
		}()

		response, ok := h.streamAdapter.GetResponse(w, r)
		if !ok {
			return
		}

		responsesChan <- response

		h.logger.Trace("Listening for messages", nil)

		for msg := range messages {
			msg.Ack()

			response, ok := h.processMessage(w, r, msg)
			if ok {
				responsesChan <- response
			}

			select {
			case <-r.Context().Done():
				return
			default:
			}
		}
	}()

	render.Respond(w, r, responsesChan)
}

func (h sseHandler) processMessage(w http.ResponseWriter, r *http.Request, msg *message.Message) (interface{}, bool) {
	ok := h.streamAdapter.Validate(r, msg)
	if !ok {
		return nil, false
	}

	h.logger.Trace("Received valid message", nil)

	return h.streamAdapter.GetResponse(w, r)
}
