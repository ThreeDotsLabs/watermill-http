package http_test

import (
	"encoding/json"
	"io/ioutil"
	netHTTP "net/http"
	"net/http/httptest"
	"testing"

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

	sseRouter, err := http.NewSSERouter(router, pubsub, watermill.NopLogger{})
	require.NoError(t, err)

	allPostsHandler := sseRouter.AddHandler("post-updated", allPostsStreamAdapter{
		allPostsRepository: postsRepositoryMock{},
	})
	postHandler := sseRouter.AddHandler("post-updated", postStreamAdapter{
		postsRepository: postsRepositoryMock{},
	})

	r := chi.NewRouter()
	r.Get("/posts", allPostsHandler.Handle)
	r.Get("/posts/{id}", postHandler.Handle)

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

	t.Run("event_stream", func(t *testing.T) {
		// TODO
	})
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
