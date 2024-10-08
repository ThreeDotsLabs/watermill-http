package http_test

import (
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	watermill_http "github.com/ThreeDotsLabs/watermill-http/v2/pkg/http"
	"github.com/ThreeDotsLabs/watermill/message"
)

var (
	metadataKey   = "key"
	metadataValue = "value"
	msgUUID       = "1"
	msgPayload    = []byte("payload")
	msg           *message.Message
)

func init() {
	msg = message.NewMessage(msgUUID, msgPayload)
	msg.Metadata.Set(metadataKey, metadataValue)
}

func TestDefaultMarshalMessageFunc(t *testing.T) {
	url := "http://some-server.domain/topic"
	req, err := watermill_http.DefaultMarshalMessageFunc(url, msg)
	require.NoError(t, err)

	assert.Equal(t, http.MethodPost, req.Method)
	assert.Equal(t, msgUUID, req.Header.Get(watermill_http.HeaderUUID))

	assert.Equal(t, url, req.URL.String())

	body, err := io.ReadAll(req.Body)
	require.NoError(t, err)
	assert.Equal(t, msgPayload, body)

	metadata := message.Metadata{}
	err = json.Unmarshal([]byte(req.Header.Get(watermill_http.HeaderMetadata)), &metadata)
	require.NoError(t, err)
	assert.Equal(t, metadataValue, metadata.Get(metadataKey))
}
