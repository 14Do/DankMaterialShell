package location

import (
	"fmt"

	"github.com/AvengeMedia/DankMaterialShell/core/internal/geolocation"
	"github.com/AvengeMedia/DankMaterialShell/core/internal/server/models"
	"github.com/AvengeMedia/dankgo/ipc/params"
)

type LocationEvent struct {
	Type string `json:"type"`
	Data State  `json:"data"`
}

func HandleRequest(conn *models.Conn, req models.Request, manager *Manager) {
	switch req.Method {
	case "location.getState":
		handleGetState(conn, req, manager)
	case "location.subscribe":
		handleSubscribe(conn, req, manager)
	case "location.setAutoEnabled":
		handleSetAutoEnabled(conn, req, manager)

	default:
		models.RespondError(conn, req.ID, fmt.Sprintf("unknown method: %s", req.Method))
	}
}

// handleSetAutoEnabled acquires/releases the weather demand for location.
// Deliberately blocking: responding only after Acquire is the ordering the
// shell's re-pull relies on to read the seeded fix. Do not offload it.
func handleSetAutoEnabled(conn *models.Conn, req models.Request, manager *Manager) {
	enabled, err := params.Bool(req.Params, "enabled")
	if err != nil {
		models.RespondError(conn, req.ID, err.Error())
		return
	}
	if dc, ok := manager.Client().(geolocation.DemandController); ok {
		if enabled {
			dc.Acquire("weather")
		} else {
			dc.Release("weather")
		}
	}
	models.Respond(conn, req.ID, models.SuccessResult{Success: true, Message: "auto location preference set"})
}

func handleGetState(conn *models.Conn, req models.Request, manager *Manager) {
	models.Respond(conn, req.ID, manager.CurrentState())
}

func handleSubscribe(conn *models.Conn, req models.Request, manager *Manager) {
	clientID := fmt.Sprintf("client-%p", conn)
	stateChan := manager.Subscribe(clientID)
	defer manager.Unsubscribe(clientID)

	initialState := manager.CurrentState()
	event := LocationEvent{
		Type: "state_changed",
		Data: initialState,
	}

	if err := conn.WriteResponse(models.Response[LocationEvent]{
		ID:     req.ID,
		Result: &event,
	}); err != nil {
		return
	}

	for state := range stateChan {
		event := LocationEvent{
			Type: "state_changed",
			Data: state,
		}
		if err := conn.WriteResponse(models.Response[LocationEvent]{
			Result: &event,
		}); err != nil {
			return
		}
	}
}
