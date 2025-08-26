package http_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	netHTTP "net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-http/v2/pkg/http"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
)

const (
	notExistingID = "not-existing-id"
	delayedID     = "delayed-id"
)

func TestSSE(t *testing.T) {
	pubsub := gochannel.NewGoChannel(gochannel.Config{}, watermill.NopLogger{})

	sseRouter, err := http.NewSSERouter(http.SSERouterConfig{
		UpstreamSubscriber: pubsub,
		ErrorHandler:       http.DefaultErrorHandler,
	}, watermill.NopLogger{})
	require.NoError(t, err)

	postUpdatedTopic := "post-updated"

	allPostsHandler := sseRouter.AddHandler(postUpdatedTopic, allPostsStreamAdapter{
		allPostsRepository: postsRepositoryMock{},
	})

	postHandler := sseRouter.AddHandler(postUpdatedTopic, postStreamAdapter{
		postsRepository: postsRepositoryMock{},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		err := sseRouter.Run(ctx)
		if err != nil {
			t.Errorf("unexpected error while starting router: %v", err)
		}
	}()

	r := chi.NewRouter()
	r.Get("/posts", allPostsHandler)
	r.Get("/posts/{id}", postHandler)

	server := httptest.NewServer(r)
	defer server.Close()

	postID := watermill.NewUUID()

	t.Run("generic_request", func(t *testing.T) {
		req, err := netHTTP.NewRequest("GET", server.URL+"/posts/"+postID, nil)
		require.NoError(t, err)

		resp, err := netHTTP.DefaultClient.Do(req)
		require.NoError(t, err)

		require.Equal(t, 200, resp.StatusCode)
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		post := Post{}
		err = json.Unmarshal(body, &post)
		require.NoError(t, err)
		require.Equal(t, postID, post.ID)
	})

	t.Run("error_request", func(t *testing.T) {
		req, err := netHTTP.NewRequest("GET", server.URL+"/posts/"+notExistingID, nil)
		require.NoError(t, err)

		resp, err := netHTTP.DefaultClient.Do(req)
		require.NoError(t, err)

		require.Equal(t, 500, resp.StatusCode)
	})

	t.Run("event_stream_error_request", func(t *testing.T) {
		req, err := netHTTP.NewRequest("GET", server.URL+"/posts/"+notExistingID, nil)
		require.NoError(t, err)

		req.Header.Add("Accept", "text/event-stream")

		resp, err := netHTTP.DefaultClient.Do(req)
		require.NoError(t, err)

		require.Equal(t, 500, resp.StatusCode)
	})

	t.Run("event_stream_no_updates", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		responses := newSSERequest(t, ctx, server.URL+"/posts/"+postID)

		response := <-responses
		post := Post{}
		err = json.Unmarshal(response, &post)
		require.NoError(t, err)
		require.Equal(t, postID, post.ID)

		response = <-responses
		require.Nil(t, response, "should receive no update")
	})

	t.Run("event_stream_updated", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		responses := newSSERequest(t, ctx, server.URL+"/posts/"+postID)

		response := <-responses
		post := Post{}
		err = json.Unmarshal(response, &post)
		require.NoError(t, err)
		require.Equal(t, postID, post.ID)

		// Publish an event
		payload, err := json.Marshal(PostUpdated{ID: postID})
		require.NoError(t, err)

		msg := message.NewMessage(watermill.NewUUID(), payload)
		err = pubsub.Publish(postUpdatedTopic, msg)
		require.NoError(t, err)

		time.Sleep(time.Millisecond * 100)

		// Should receive an update
		response = <-responses
		post = Post{}
		err = json.Unmarshal(response, &post)
		require.NoError(t, err)
		require.Equal(t, postID, post.ID)

		// Publish an event with different post ID
		payload, err = json.Marshal(PostUpdated{ID: watermill.NewUUID()})
		require.NoError(t, err)

		msg = message.NewMessage(watermill.NewUUID(), payload)
		err = pubsub.Publish(postUpdatedTopic, msg)
		require.NoError(t, err)

		time.Sleep(time.Millisecond * 100)

		response = <-responses
		require.Nil(t, response, "should receive no update")
	})

	t.Run("event_stream_updated_after_context_closed", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		responses := newSSERequest(t, ctx, server.URL+"/posts/"+delayedID)

		response, ok := <-responses
		require.True(t, ok)
		post := Post{}
		err = json.Unmarshal(response, &post)
		require.NoError(t, err)
		require.Equal(t, delayedID, post.ID)

		payload, err := json.Marshal(PostUpdated{ID: delayedID})
		require.NoError(t, err)

		msg := message.NewMessage(watermill.NewUUID(), payload)
		err = pubsub.Publish(postUpdatedTopic, msg)
		require.NoError(t, err)

		// Wait until ByID() starts processing
		time.Sleep(time.Millisecond * 100)

		cancel()

		// Wait until ByID() finishes
		time.Sleep(time.Millisecond * 150)

		select {
		case response, ok = <-responses:
			assert.False(t, ok)
			require.Emptyf(t, response, "should receive no update, got %s", string(response))
		default:
		}
	})
}

func newSSERequest(t *testing.T, ctx context.Context, url string) chan []byte {
	req, err := netHTTP.NewRequest("GET", url, nil)
	require.NoError(t, err)

	req = req.WithContext(ctx)
	req.Header.Add("Accept", "text/event-stream")

	resp, err := netHTTP.DefaultClient.Do(req)
	require.NoError(t, err)

	require.Equal(t, 200, resp.StatusCode)

	br := bufio.NewReader(resp.Body)

	responsesChan := make(chan []byte)

	go func() {
		defer close(responsesChan)
		defer resp.Body.Close()

		for {
			line, err := br.ReadBytes('\n')
			if err != nil {
				if err != io.EOF && err != context.DeadlineExceeded && err != context.Canceled {
					t.Errorf("unexpected error: %v", err)
				}

				return
			}

			if len(line) < 2 {
				continue
			}

			parts := bytes.Split(line, []byte(": "))
			if len(parts) != 2 {
				t.Errorf("expected 2 parts, got %v: %v", len(parts), parts)
				return
			}

			switch string(parts[0]) {
			case "event":
				eventName := strings.TrimSpace(string(parts[1]))
				if eventName != "data" {
					t.Errorf("expected data, got %v instead", eventName)
					return
				}
			case "data":
				responsesChan <- parts[1]
			}

			select {
			case <-ctx.Done():
				return
			default:
			}
		}
	}()

	return responsesChan
}

type Post struct {
	ID      string
	Content string
}

type PostUpdated struct {
	ID string
}

type postStreamAdapter struct {
	postsRepository interface {
		ByID(id string) (Post, error)
	}
}

func (s postStreamAdapter) InitialStreamResponse(w netHTTP.ResponseWriter, r *netHTTP.Request) (response interface{}, ok bool) {
	postID := chi.URLParam(r, "id")

	post, err := s.postsRepository.ByID(postID)
	if err != nil {
		w.WriteHeader(500)
		return nil, false
	}

	return post, true
}

func (s postStreamAdapter) NextStreamResponse(r *netHTTP.Request, msg *message.Message) (response interface{}, ok bool) {
	postUpdated := PostUpdated{}

	err := json.Unmarshal(msg.Payload, &postUpdated)
	if err != nil {
		return nil, false
	}

	postID := chi.URLParam(r, "id")

	if postUpdated.ID != postID {
		return nil, false
	}

	post, err := s.postsRepository.ByID(postID)
	if err != nil {
		return nil, false
	}

	return post, true
}

type allPostsStreamAdapter struct {
	allPostsRepository interface {
		All() ([]Post, error)
	}
}

func (s allPostsStreamAdapter) InitialStreamResponse(w netHTTP.ResponseWriter, r *netHTTP.Request) (response interface{}, ok bool) {
	posts, err := s.allPostsRepository.All()
	if err != nil {
		w.WriteHeader(500)
		return nil, false
	}

	return posts, true
}

func (s allPostsStreamAdapter) NextStreamResponse(r *netHTTP.Request, msg *message.Message) (response interface{}, ok bool) {
	posts, err := s.allPostsRepository.All()
	if err != nil {
		return nil, false
	}

	return posts, true
}

type postsRepositoryMock struct{}

func (p postsRepositoryMock) ByID(id string) (Post, error) {
	if id == notExistingID {
		return Post{}, errors.New("post doesn't exist")
	}

	if id == delayedID {
		time.Sleep(time.Millisecond * 200)
	}

	return Post{
		ID:      id,
		Content: "some content",
	}, nil
}

func (p postsRepositoryMock) All() ([]Post, error) {
	return []Post{
		{
			ID:      "1",
			Content: "some content",
		},
		{
			ID:      "2",
			Content: "some content",
		},
	}, nil
}
