package main

import (
	"github.com/AvengeMedia/DankMaterialShell/core/internal/log"
	"github.com/AvengeMedia/DankMaterialShell/core/internal/traywatcher"
	"github.com/spf13/cobra"
)

var trayWatcherCmd = &cobra.Command{
	Use:   "tray-watcher",
	Short: "Run a minimal StatusNotifierWatcher for early tray registration",
	Long: `Run a minimal org.kde.StatusNotifierWatcher daemon.

Started early in the session (Before=graphical-session.target via
dms-tray-watcher.service), it lets tray apps launched by XDG autostart
register their items before the shell finishes loading, and keeps them
registered across shell restarts. The shell's tray host picks items up from
this watcher; if it is not running, the shell's built-in watcher takes over.`,
	Run: func(cmd *cobra.Command, args []string) {
		if err := traywatcher.Run(); err != nil {
			log.Fatalf("%v", err)
		}
	},
}
