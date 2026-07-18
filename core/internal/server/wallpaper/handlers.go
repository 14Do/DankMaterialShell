package wallpaper

import (
	"encoding/json"
	"fmt"

	"github.com/AvengeMedia/DankMaterialShell/core/internal/server/models"
	"github.com/AvengeMedia/dankgo/ipc/params"
)

func HandleRequest(conn *models.Conn, req models.Request, manager *Manager) {
	if manager == nil {
		models.RespondError(conn, req.ID, "wallpaper manager not initialized")
		return
	}

	switch req.Method {
	case "wallpaper.getState":
		handleGetState(conn, req, manager)
	case "wallpaper.setConfig":
		handleSetConfig(conn, req, manager)
	case "wallpaper.trigger":
		handleTrigger(conn, req, manager)
	case "wallpaper.subscribe":
		handleSubscribe(conn, req, manager)
	default:
		models.RespondError(conn, req.ID, fmt.Sprintf("unknown method: %s", req.Method))
	}
}

func handleGetState(conn *models.Conn, req models.Request, manager *Manager) {
	models.Respond(conn, req.ID, manager.GetState())
}

func handleSetConfig(conn *models.Conn, req models.Request, manager *Manager) {
	raw, ok := params.Any(req.Params, "config")
	if !ok {
		models.RespondError(conn, req.ID, "missing or invalid 'config' parameter")
		return
	}

	data, err := json.Marshal(raw)
	if err != nil {
		models.RespondError(conn, req.ID, err.Error())
		return
	}

	var config Config
	if err := json.Unmarshal(data, &config); err != nil {
		models.RespondError(conn, req.ID, err.Error())
		return
	}

	manager.SetConfig(config)
	models.Respond(conn, req.ID, models.SuccessResult{Success: true, Message: "wallpaper schedule set"})
}

func handleTrigger(conn *models.Conn, req models.Request, manager *Manager) {
	manager.ResetSchedule(params.StringOpt(req.Params, "target", ""))
	models.Respond(conn, req.ID, models.SuccessResult{Success: true, Message: "wallpaper schedule reset"})
}

func handleSubscribe(conn *models.Conn, req models.Request, manager *Manager) {
	clientID := fmt.Sprintf("client-%p", conn)
	stateChan := manager.Subscribe(clientID)
	defer manager.Unsubscribe(clientID)

	initialState := manager.GetState()
	if err := conn.WriteResponse(models.Response[State]{
		ID:     req.ID,
		Result: &initialState,
	}); err != nil {
		return
	}

	for state := range stateChan {
		if err := conn.WriteResponse(models.Response[State]{
			Result: &state,
		}); err != nil {
			return
		}
	}
}
