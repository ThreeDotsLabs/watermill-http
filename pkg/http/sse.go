package http

import (
	"net/http"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/go-chi/render"
)

type StreamAdapter interface {
	// ResponseProvider returns the response to be sent back to client.
	// Any errors that occur should be handled and written to `w`, returning false as `ok`.
	ResponseProvider(w http.ResponseWriter, r *http.Request) (response interface{}, ok bool)
	// Validator validates if the incoming message should be handled by this handler.
	// Typically this involves checking some kind of model ID.
	Validator(r *http.Request, msg *message.Message) (ok bool)
}

type SSEHandler struct {
	subscriber    message.Subscriber
	topic         string
	streamAdapter StreamAdapter
}

func NewSSEHandler(
	subscriber message.Subscriber,
	topic string,
	streamAdapter StreamAdapter,
) SSEHandler {
	return SSEHandler{
		subscriber:    subscriber,
		topic:         topic,
		streamAdapter: streamAdapter,
	}
}

func (s SSEHandler) Handler(w http.ResponseWriter, r *http.Request) {
	if render.GetAcceptedContentType(r) == render.ContentTypeEventStream {
		s.handleEventStream(w, r)
		return
	}

	s.handleGenericRequest(w, r)
}

func (s SSEHandler) handleGenericRequest(w http.ResponseWriter, r *http.Request) {
	response, ok := s.streamAdapter.ResponseProvider(w, r)
	if !ok {
		return
	}

	render.Respond(w, r, response)
}

func (s SSEHandler) handleEventStream(w http.ResponseWriter, r *http.Request) {
	messages, err := s.subscriber.Subscribe(r.Context(), s.topic)
	if err != nil {
		w.WriteHeader(500)
		return
	}

	responsesChan := make(chan interface{})

	go func() {
		defer close(responsesChan)

		response, ok := s.streamAdapter.ResponseProvider(w, r)
		if !ok {
			return
		}
		responsesChan <- response

		for msg := range messages {
			ok := s.streamAdapter.Validator(r, msg)
			if !ok {
				msg.Ack()
				continue
			}

			response, ok := s.streamAdapter.ResponseProvider(w, r)
			if !ok {
				return
			}

			select {
			case <-r.Context().Done():
				break
			default:
			}

			responsesChan <- response
			msg.Ack()
		}
	}()

	render.Respond(w, r, responsesChan)
}
