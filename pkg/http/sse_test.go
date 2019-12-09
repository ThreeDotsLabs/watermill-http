package http_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	netHTTP "net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-http/pkg/http"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
	"github.com/go-chi/chi"
	"github.com/stretchr/testify/require"
)

func TestSSE(t *testing.T) {
	router, err := message.NewRouter(message.RouterConfig{}, watermill.NopLogger{})
	require.NoError(t, err)

	pubsub := gochannel.NewGoChannel(gochannel.Config{}, watermill.NopLogger{})

	sseRouter, err := http.NewSSERouter(router, pubsub, http.DefaultErrorHandler, watermill.NopLogger{})
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
		err := router.Run(ctx)
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
		body, err := ioutil.ReadAll(resp.Body)
		require.NoError(t, err)

		post := Post{}
		err = json.Unmarshal(body, &post)
		require.NoError(t, err)
		require.Equal(t, postID, post.ID)
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

func (s postStreamAdapter) GetResponse(w netHTTP.ResponseWriter, r *netHTTP.Request) (response interface{}, ok bool) {
	postID := chi.URLParam(r, "id")

	post, err := s.postsRepository.ByID(postID)
	if err != nil {
		w.WriteHeader(500)
		return nil, false
	}

	return post, true
}

func (s postStreamAdapter) Validate(r *netHTTP.Request, msg *message.Message) (ok bool) {
	postUpdated := PostUpdated{}

	err := json.Unmarshal(msg.Payload, &postUpdated)
	if err != nil {
		return false
	}

	postID := chi.URLParam(r, "id")

	return postUpdated.ID == postID
}

type allPostsStreamAdapter struct {
	allPostsRepository interface {
		All() ([]Post, error)
	}
}

func (s allPostsStreamAdapter) GetResponse(w netHTTP.ResponseWriter, r *netHTTP.Request) (response interface{}, ok bool) {
	posts, err := s.allPostsRepository.All()
	if err != nil {
		w.WriteHeader(500)
		return nil, false
	}

	return posts, true
}

func (s allPostsStreamAdapter) Validate(r *netHTTP.Request, msg *message.Message) (ok bool) {
	return true
}

type postsRepositoryMock struct{}

func (p postsRepositoryMock) ByID(id string) (Post, error) {
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
