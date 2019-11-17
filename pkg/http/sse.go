package http

import (
	"fmt"
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

type SSERouter struct {
	internalPubSub *gochannel.GoChannel

	upstreamRouter     *message.Router
	upstreamSubscriber message.Subscriber
}

func NewSSERouter(
	upstreamRouter *message.Router,
	upstreamSubscriber message.Subscriber,
	logger watermill.LoggerAdapter,
) (SSERouter, error) {
	return SSERouter{
		internalPubSub: gochannel.NewGoChannel(gochannel.Config{}, logger),

		upstreamRouter:     upstreamRouter,
		upstreamSubscriber: upstreamSubscriber,
	}, nil
}

func (r SSERouter) AddHandler(topic string, streamAdapter StreamAdapter) SSEHandler {
	r.upstreamRouter.AddHandler(
		// TODO come up with better handler name?
		fmt.Sprintf("sse_%s_%s", topic, watermill.NewUUID()),
		topic,
		r.upstreamSubscriber,
		topic,
		r.internalPubSub,
		func(msg *message.Message) (messages []*message.Message, e error) {
			return []*message.Message{msg}, nil
		},
	)

	return newSSEHandler(r.internalPubSub, topic, streamAdapter)
}

type SSEHandler struct {
	subscriber    message.Subscriber
	topic         string
	streamAdapter StreamAdapter
}

func newSSEHandler(
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

func (s SSEHandler) Handle(w http.ResponseWriter, r *http.Request) {
	if render.GetAcceptedContentType(r) == render.ContentTypeEventStream {
		s.handleEventStream(w, r)
		return
	}

	s.handleGenericRequest(w, r)
}

func (s SSEHandler) handleGenericRequest(w http.ResponseWriter, r *http.Request) {
	response, ok := s.streamAdapter.GetResponse(w, r)
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

		response, ok := s.streamAdapter.GetResponse(w, r)
		if !ok {
			return
		}
		responsesChan <- response

		for msg := range messages {
			ok := s.streamAdapter.Validate(r, msg)
			if !ok {
				msg.Ack()
				continue
			}

			response, ok := s.streamAdapter.GetResponse(w, r)
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
