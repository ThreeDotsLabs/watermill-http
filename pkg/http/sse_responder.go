package http

import (
	"fmt"
	"net/http"
	"reflect"
	"strings"

	"github.com/go-chi/render"
)

type sseResponder struct {
	marshaler SSEMarshaler
}

// Respond handles streaming JSON and XML responses, automatically setting the
// Content-Type based on request headers.
// Based on go-chi/render.
func (s sseResponder) Respond(w http.ResponseWriter, r *http.Request, v interface{}) {
	if v != nil {
		switch reflect.TypeOf(v).Kind() {
		case reflect.Chan:
			switch render.GetAcceptedContentType(r) {
			case render.ContentTypeEventStream:
				s.channelEventStream(w, r, v)
				return
			default:
				v = s.channelIntoSlice(w, r, v)
			}
		}
	}

	// Format response based on request Accept header.
	switch render.GetAcceptedContentType(r) {
	case render.ContentTypeJSON:
		render.JSON(w, r, v)
	case render.ContentTypeXML:
		render.XML(w, r, v)
	default:
		render.JSON(w, r, v)
	}
}

func (s sseResponder) channelEventStream(w http.ResponseWriter, r *http.Request, v interface{}) {
	if reflect.TypeOf(v).Kind() != reflect.Chan {
		panic(fmt.Sprintf("render: event stream expects a channel, not %v", reflect.TypeOf(v).Kind()))
	}

	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")

	// Disable proxy buffering for stream responses
	w.Header().Set("X-Accel-Buffering", "no")

	if r.ProtoMajor == 1 {
		// An endpoint MUST NOT generate an HTTP/2 message containing connection-specific header fields.
		// Source: RFC7540
		w.Header().Set("Connection", "keep-alive")
	}

	w.WriteHeader(http.StatusOK)

	ctx := r.Context()
	for {
		switch chosen, recv, ok := reflect.Select([]reflect.SelectCase{
			{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ctx.Done())},
			{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(v)},
		}); chosen {
		case 0: // equivalent to: case <-ctx.Done()
			_, _ = w.Write([]byte("event: error\ndata: {\"error\":\"Server Timeout\"}\n\n"))
			return

		default: // equivalent to: case v, ok := <-stream
			if !ok {
				_, _ = w.Write([]byte("event: EOF\n\n"))
				return
			}
			v := recv.Interface()

			// Build each channel item.
			if rv, ok := v.(render.Renderer); ok {
				err := renderer(w, r, rv)
				if err != nil {
					v = err
				} else {
					v = rv
				}
			}

			event, ok := v.(ServerSentEvent)
			if !ok {
				var err error
				event, err = s.marshaler.Marshal(ctx, v)
				if err != nil {
					_, _ = w.Write([]byte(fmt.Sprintf("event: error\ndata: {\"error\":\"%v\"}\n\n", err)))
					if f, ok := w.(http.Flusher); ok {
						f.Flush()
					}
					continue
				}
			}

			data := strings.Join(strings.Split(string(event.Data), "\n"), "\ndata: ")

			_, _ = w.Write([]byte(fmt.Sprintf("event: %s\ndata: %s\n\n", event.Event, data)))
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		}
	}
}

// channelIntoSlice buffers channel data into a slice.
func (s sseResponder) channelIntoSlice(w http.ResponseWriter, r *http.Request, from interface{}) interface{} {
	ctx := r.Context()

	var to []interface{}
	for {
		switch chosen, recv, ok := reflect.Select([]reflect.SelectCase{
			{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ctx.Done())},
			{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(from)},
		}); chosen {
		case 0: // equivalent to: case <-ctx.Done()
			http.Error(w, "Server Timeout", http.StatusGatewayTimeout)
			return nil

		default: // equivalent to: case v, ok := <-stream
			if !ok {
				return to
			}
			v := recv.Interface()

			// Render each channel item.
			if rv, ok := v.(render.Renderer); ok {
				err := renderer(w, r, rv)
				if err != nil {
					v = err
				} else {
					v = rv
				}
			}

			to = append(to, v)
		}
	}
}

var (
	rendererType = reflect.TypeOf(new(render.Renderer)).Elem()
)

func renderer(w http.ResponseWriter, r *http.Request, v render.Renderer) error {
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}

	// We call it top-down.
	if err := v.Render(w, r); err != nil {
		return err
	}

	// We're done if the Renderer isn't a struct object
	if rv.Kind() != reflect.Struct {
		return nil
	}

	// For structs, we call Render on each field that implements Renderer
	for i := 0; i < rv.NumField(); i++ {
		f := rv.Field(i)
		if f.Type().Implements(rendererType) {

			if isNil(f) {
				continue
			}

			fv := f.Interface().(render.Renderer)
			if err := renderer(w, r, fv); err != nil {
				return err
			}

		}
	}

	return nil
}

func isNil(f reflect.Value) bool {
	switch f.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		return f.IsNil()
	default:
		return false
	}
}
