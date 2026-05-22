package http

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"reflect"
	"strings"
)

const (
	contentTypeEventStream = "text/event-stream"
	contentTypeJSON        = "application/json"
	contentTypeXML         = "application/xml"
)

// acceptedContentType parses the Accept header and returns the first supported
// content type. Defaults to application/json when none match.
func acceptedContentType(r *http.Request) string {
	accept := r.Header.Get("Accept")
	for _, raw := range strings.Split(accept, ",") {
		t := strings.TrimSpace(raw)
		if i := strings.Index(t, ";"); i >= 0 {
			t = strings.TrimSpace(t[:i])
		}
		switch t {
		case contentTypeEventStream, contentTypeJSON, contentTypeXML:
			return t
		}
	}
	return contentTypeJSON
}

type sseResponder struct {
	marshaler SSEMarshaler
}

// Respond handles streaming JSON and XML responses, automatically setting the
// Content-Type based on request headers.
func (s sseResponder) Respond(w http.ResponseWriter, r *http.Request, v interface{}) {
	if v != nil {
		switch reflect.TypeOf(v).Kind() {
		case reflect.Chan:
			if acceptedContentType(r) == contentTypeEventStream {
				s.channelEventStream(w, r, v)
				return
			}
			v = s.channelIntoSlice(w, r, v)
		}
	}

	switch acceptedContentType(r) {
	case contentTypeXML:
		w.Header().Set("Content-Type", "application/xml; charset=utf-8")
		_ = xml.NewEncoder(w).Encode(v)
	default:
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		_ = json.NewEncoder(w).Encode(v)
	}
}

func (s sseResponder) channelEventStream(w http.ResponseWriter, r *http.Request, v interface{}) {
	if reflect.TypeOf(v).Kind() != reflect.Chan {
		panic(fmt.Sprintf("sse: event stream expects a channel, not %v", reflect.TypeOf(v).Kind()))
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
		case 0:
			_, _ = w.Write([]byte("event: error\ndata: {\"error\":\"Server Timeout\"}\n\n"))
			return

		default:
			if !ok {
				_, _ = w.Write([]byte("event: EOF\n\n"))
				return
			}
			v := recv.Interface()

			event, ok := v.(ServerSentEvent)
			if !ok {
				var err error
				event, err = s.marshaler.Marshal(ctx, v)
				if err != nil {
					_, _ = fmt.Fprintf(w, "event: error\ndata: {\"error\":\"%v\"}\n\n", err)
					if f, ok := w.(http.Flusher); ok {
						f.Flush()
					}
					continue
				}
			}

			data := strings.Join(strings.Split(string(event.Data), "\n"), "\ndata: ")

			_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event.Event, data)
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
		case 0:
			http.Error(w, "Server Timeout", http.StatusGatewayTimeout)
			return nil

		default:
			if !ok {
				return to
			}
			to = append(to, recv.Interface())
		}
	}
}
