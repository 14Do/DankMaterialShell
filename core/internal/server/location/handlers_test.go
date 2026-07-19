package location

import (
	"encoding/json"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/AvengeMedia/DankMaterialShell/core/internal/server/models"
)

// gatedDemandStub blocks Acquire until the gate opens (facade mid-acquisition).
type gatedDemandStub struct {
	*stubClient
	gate     chan struct{}
	acquired chan struct{}

	demandMu sync.Mutex
	demand   []string // "acquire:<source>" / "release:<source>" in call order
}

func newGatedDemandStub() *gatedDemandStub {
	return &gatedDemandStub{
		stubClient: newStubClient(),
		gate:       make(chan struct{}),
		acquired:   make(chan struct{}),
	}
}

func (c *gatedDemandStub) Acquire(source string) {
	c.demandMu.Lock()
	c.demand = append(c.demand, "acquire:"+source)
	c.demandMu.Unlock()
	close(c.acquired)
	<-c.gate
}

func (c *gatedDemandStub) Release(source string) {
	c.demandMu.Lock()
	c.demand = append(c.demand, "release:"+source)
	c.demandMu.Unlock()
}

func (c *gatedDemandStub) demandCalls() []string {
	c.demandMu.Lock()
	defer c.demandMu.Unlock()
	return append([]string(nil), c.demand...)
}

// The handler must not respond until Acquire returns (the shell re-pull ordering).
// net.Pipe is unbuffered, so an early response shows up as readable bytes.
func TestHandleSetAutoEnabled_RespondsOnlyAfterAcquire(t *testing.T) {
	stub := newGatedDemandStub()
	m, err := NewManager(stub)
	require.NoError(t, err)
	defer m.Close()

	server, client := net.Pipe()
	defer client.Close()

	handlerDone := make(chan struct{})
	go func() {
		defer close(handlerDone)
		defer server.Close()
		handleSetAutoEnabled(models.NewConn(server), models.Request{
			ID:     7,
			Method: "location.setAutoEnabled",
			Params: map[string]any{"enabled": true},
		}, m)
	}()

	select {
	case <-stub.acquired:
	case <-time.After(time.Second):
		t.Fatal("handler never called Acquire")
	}

	// Handler is parked inside Acquire; nothing may have been written yet.
	require.NoError(t, client.SetReadDeadline(time.Now().Add(150*time.Millisecond)))
	buf := make([]byte, 1)
	_, err = client.Read(buf)
	require.ErrorIs(t, err, os.ErrDeadlineExceeded,
		"response bytes appeared before Acquire returned")

	close(stub.gate) // acquisition completes

	require.NoError(t, client.SetReadDeadline(time.Now().Add(time.Second)))
	var resp models.Response[models.SuccessResult]
	require.NoError(t, json.NewDecoder(client).Decode(&resp))
	assert.Equal(t, 7, resp.ID)
	require.NotNil(t, resp.Result)
	assert.True(t, resp.Result.Success)
	assert.Equal(t, []string{"acquire:weather"}, stub.demandCalls())

	<-handlerDone
}

func TestHandleSetAutoEnabled_DisableReleasesWeatherDemand(t *testing.T) {
	stub := newGatedDemandStub()
	m, err := NewManager(stub)
	require.NoError(t, err)
	defer m.Close()

	server, client := net.Pipe()
	defer client.Close()

	handlerDone := make(chan struct{})
	go func() {
		defer close(handlerDone)
		defer server.Close()
		handleSetAutoEnabled(models.NewConn(server), models.Request{
			ID:     8,
			Method: "location.setAutoEnabled",
			Params: map[string]any{"enabled": false},
		}, m)
	}()

	require.NoError(t, client.SetReadDeadline(time.Now().Add(time.Second)))
	var resp models.Response[models.SuccessResult]
	require.NoError(t, json.NewDecoder(client).Decode(&resp))
	assert.Equal(t, 8, resp.ID)
	require.NotNil(t, resp.Result)
	assert.True(t, resp.Result.Success)
	assert.Equal(t, []string{"release:weather"}, stub.demandCalls())

	<-handlerDone
}
