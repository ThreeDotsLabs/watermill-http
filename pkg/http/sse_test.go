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
	pubsub := gochannel.NewGoChannel(gochannel.Config{}, watermill.NopLogger{})

	streamAdapter := streamAdapter{
		postsRepository: postsRepositoryMock{},
	}

	h := http.NewSSEHandler(
		pubsub,
		"topic",
		streamAdapter,
	)

	postID := watermill.NewUUID()

	r := chi.NewRouter()
	r.Get("/posts/{id}", h.Handler)

	server := httptest.NewServer(r)
	defer server.Close()

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

type postsRepository interface {
	ByID(id string) (Post, error)
}

type streamAdapter struct {
	postsRepository postsRepository
}

func (s streamAdapter) ResponseProvider(w netHTTP.ResponseWriter, r *netHTTP.Request) (response interface{}, ok bool) {
	postID := chi.URLParam(r, "id")

	post, err := s.postsRepository.ByID(postID)
	if err != nil {
		w.WriteHeader(500)
		return nil, false
	}

	return post, true
}

func (s streamAdapter) Validator(r *netHTTP.Request, msg *message.Message) (ok bool) {
	postUpdated := PostUpdated{}

	err := json.Unmarshal(msg.Payload, &postUpdated)
	if err != nil {
		return false
	}

	postID := chi.URLParam(r, "id")

	return postUpdated.ID == postID
}

type postsRepositoryMock struct{}

func (p postsRepositoryMock) ByID(id string) (Post, error) {
	return Post{
		ID:      id,
		Content: "some content",
	}, nil
}
