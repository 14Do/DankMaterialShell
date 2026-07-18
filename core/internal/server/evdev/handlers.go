package evdev

import (
	"github.com/AvengeMedia/DankMaterialShell/core/internal/server/models"
)

func HandleRequest(conn *models.Conn, req models.Request, m *Manager) {
	switch req.Method {
	case "evdev.getState":
		handleGetState(conn, req, m)
	default:
		models.RespondError(conn, req.ID, "unknown method: "+req.Method)
	}
}

func handleGetState(conn *models.Conn, req models.Request, m *Manager) {
	models.Respond(conn, req.ID, m.GetState())
}
