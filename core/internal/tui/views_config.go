package tui

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/AvengeMedia/DankMaterialShell/core/internal/config"
	"github.com/AvengeMedia/DankMaterialShell/core/internal/deps"
	"github.com/AvengeMedia/DankMaterialShell/core/internal/distros"
	tea "github.com/charmbracelet/bubbletea"
)

type configDeploymentResult struct {
	results []config.DeploymentResult
	error   error
}

type ExistingConfigInfo struct {
	ConfigType string
	Path       string
	Exists     bool
}

type configCheckResult struct {
	configs []ExistingConfigInfo
	error   error
}

func (m Model) viewDeployingConfigs() string {
	var b strings.Builder

	b.WriteString(m.renderBanner())
	b.WriteString("\n")

	title := m.styles.Title.Render("Deploying Configurations")
	b.WriteString(title)
	b.WriteString("\n\n")

	spinner := m.spinner.View()
	status := m.styles.Normal.Render("Setting up configuration files...")
	fmt.Fprintf(&b, "%s %s", spinner, status)
	b.WriteString("\n\n")

	// Show progress information
	info := m.styles.Subtle.Render("• Creating backups of existing configurations\n• Deploying optimized configurations\n• Detecting system paths")
	b.WriteString(info)

	// Show live log output if available
	if len(m.installationLogs) > 0 {
		b.WriteString("\n\n")
		logHeader := m.styles.Subtle.Render("Configuration Log:")
		b.WriteString(logHeader)
		b.WriteString("\n")

		// Show last few lines of logs
		maxLines := 5
		startIdx := 0
		if len(m.installationLogs) > maxLines {
			startIdx = len(m.installationLogs) - maxLines
		}

		for i := startIdx; i < len(m.installationLogs); i++ {
			if m.installationLogs[i] != "" {
				logLine := m.styles.Subtle.Render("  " + m.installationLogs[i])
				b.WriteString(logLine)
				b.WriteString("\n")
			}
		}
	}

	return b.String()
}

func (m Model) updateDeployingConfigsState(msg tea.Msg) (tea.Model, tea.Cmd) {
	if result, ok := msg.(configDeploymentResult); ok {
		if result.error != nil {
			m.err = result.error
			m.state = StateError
			m.isLoading = false
			return m, nil
		}

		for _, deployResult := range result.results {
			if deployResult.Deployed {
				logMsg := fmt.Sprintf("✓ %s configuration deployed", deployResult.ConfigType)
				if deployResult.BackupPath != "" {
					logMsg += fmt.Sprintf(" (backup: %s)", deployResult.BackupPath)
				}
				m.installationLogs = append(m.installationLogs, logMsg)
			}
		}

		m.state = StateInstallComplete
		m.isLoading = false
		return m, nil
	}

	return m, m.listenForLogs()
}

func (m Model) deployConfigurations() tea.Cmd {
	return func() tea.Msg {
		// Determine the selected window manager
		wm := m.selectedWindowManager()

		// Determine the selected terminal
		var terminal deps.Terminal
		if m.osInfo != nil && m.osInfo.Distribution.ID == "gentoo" {
			switch m.selectedTerminal {
			case 0:
				terminal = deps.TerminalKitty
			case 1:
				terminal = deps.TerminalAlacritty
			default:
				terminal = deps.TerminalKitty
			}
		} else {
			switch m.selectedTerminal {
			case 0:
				terminal = deps.TerminalGhostty
			case 1:
				terminal = deps.TerminalKitty
			default:
				terminal = deps.TerminalAlacritty
			}
		}

		deployer := config.NewConfigDeployer(m.logChan)

		results, err := deployer.DeployConfigurationsSelectiveWithReinstallsAndSystemd(context.Background(), wm, terminal, m.dependencies, m.replaceConfigs, m.reinstallItems, m.useSystemdConfig())

		return configDeploymentResult{
			results: results,
			error:   err,
		}
	}
}

func (m Model) optionalDepSelected(name string) bool {
	if m.disabledItems[name] {
		return false
	}
	for _, dep := range m.dependencies {
		if dep.Name == name {
			return true
		}
	}
	return false
}

func (m Model) useSystemdConfig() bool {
	if m.osInfo == nil {
		return true
	}
	distroConfig, exists := distros.Registry[m.osInfo.Distribution.ID]
	if !exists {
		return true
	}
	return distroConfig.Family != distros.FamilyVoid
}

func (m Model) viewConfigConfirmation() string {
	var b strings.Builder

	b.WriteString(m.renderBanner())
	b.WriteString("\n")

	title := m.styles.Title.Render("Configuration Deployment")
	b.WriteString(title)
	b.WriteString("\n\n")

	if len(m.existingConfigs) == 0 {
		// No existing configs, proceed directly
		info := m.styles.Normal.Render("No existing configurations found. Proceeding with deployment...")
		b.WriteString(info)
		return b.String()
	}

	// Show existing configurations with toggle options
	for i, configInfo := range m.existingConfigs {
		if configInfo.Exists {
			var status string
			var replaceMarker string

			shouldReplace := m.replaceConfigs[configInfo.ConfigType]
			if _, exists := m.replaceConfigs[configInfo.ConfigType]; !exists {
				shouldReplace = true
				m.replaceConfigs[configInfo.ConfigType] = true
			}

			if shouldReplace {
				replaceMarker = "🔄 "
				status = m.styles.Warning.Render("Will replace")
			} else {
				replaceMarker = "✓ "
				status = m.styles.Success.Render("Keep existing")
			}

			var line string
			if i == m.selectedConfig {
				line = fmt.Sprintf("▶ %s%-15s %s", replaceMarker, configInfo.ConfigType, status)
				line += fmt.Sprintf("\n    %s", configInfo.Path)
				line = m.styles.SelectedOption.Render(line)
			} else {
				line = fmt.Sprintf("  %s%-15s %s", replaceMarker, configInfo.ConfigType, status)
				line += fmt.Sprintf("\n    %s", configInfo.Path)
				line = m.styles.Normal.Render(line)
			}

			b.WriteString(line)
			b.WriteString("\n\n")
		}
	}

	backup := m.styles.Success.Render("✓ Replaced configurations will be backed up with timestamp")
	b.WriteString(backup)
	b.WriteString("\n\n")

	if note := m.configReplacementNote(); note != "" {
		b.WriteString(m.styles.Subtle.Render(note))
		b.WriteString("\n\n")
	}

	help := m.styles.Subtle.Render("↑/↓: Navigate, Space: Toggle replace/keep, Enter: Continue")
	b.WriteString(help)

	return b.String()
}

func (m Model) configReplacementNote() string {
	if m.selectedConfig < 0 || m.selectedConfig >= len(m.existingConfigs) {
		return ""
	}
	configInfo := m.existingConfigs[m.selectedConfig]
	if !configInfo.Exists {
		return ""
	}

	switch configInfo.ConfigType {
	case "Niri":
		if m.useSystemdConfig() {
			return "Replacing Niri writes the DMS Niri template and uses the user systemd dms service for shell autostart."
		}
		return `Replacing Niri writes the DMS Niri template and starts DMS from Niri with spawn-at-startup "dms" "run".`
	case "Hyprland":
		if m.useSystemdConfig() {
			return "Replacing Hyprland writes the DMS Lua template and uses the user systemd dms service for shell autostart."
		}
		return `Replacing Hyprland writes the DMS Lua template and starts DMS from Hyprland with hl.exec_cmd("dms run").`
	case "Mango":
		return "Replacing Mango writes the DMS Mango template and starts DMS from Mango with exec-once=dms run."
	case "Ghostty":
		return "Replacing Ghostty writes the DMS terminal defaults and theme include."
	case "Kitty":
		return "Replacing Kitty writes the DMS terminal defaults, theme include, and tab styling."
	case "Alacritty":
		return "Replacing Alacritty writes the DMS terminal defaults and theme import."
	default:
		return ""
	}
}

func (m Model) updateConfigConfirmationState(msg tea.Msg) (tea.Model, tea.Cmd) {
	if result, ok := msg.(configCheckResult); ok {
		if result.error != nil {
			m.err = result.error
			m.state = StateError
			return m, nil
		}

		m.existingConfigs = result.configs

		firstExistingSet := false
		for i, config := range result.configs {
			if config.Exists {
				m.replaceConfigs[config.ConfigType] = true
				if !firstExistingSet {
					m.selectedConfig = i
					firstExistingSet = true
				}
			}
		}

		hasExisting := false
		for _, config := range result.configs {
			if config.Exists {
				hasExisting = true
				break
			}
		}

		if !hasExisting {
			// No existing configs, proceed directly to deployment
			m.state = StateDeployingConfigs
			return m, m.deployConfigurations()
		}

		// Show confirmation view
		return m, nil
	}

	if keyMsg, ok := msg.(tea.KeyMsg); ok {
		switch keyMsg.String() {
		case "up":
			if m.selectedConfig > 0 {
				for i := m.selectedConfig - 1; i >= 0; i-- {
					if m.existingConfigs[i].Exists {
						m.selectedConfig = i
						break
					}
				}
			}
		case "down":
			if m.selectedConfig < len(m.existingConfigs)-1 {
				for i := m.selectedConfig + 1; i < len(m.existingConfigs); i++ {
					if m.existingConfigs[i].Exists {
						m.selectedConfig = i
						break
					}
				}
			}
		case " ":
			if len(m.existingConfigs) > 0 && m.selectedConfig < len(m.existingConfigs) {
				configType := m.existingConfigs[m.selectedConfig].ConfigType
				if m.existingConfigs[m.selectedConfig].Exists {
					m.replaceConfigs[configType] = !m.replaceConfigs[configType]
				}
			}
		case "enter":
			m.state = StateDeployingConfigs
			return m, m.deployConfigurations()
		}
	}

	return m, nil
}

func (m Model) checkExistingConfigurations() tea.Cmd {
	return func() tea.Msg {
		var configs []ExistingConfigInfo

		switch m.selectedWindowManager() {
		case deps.WindowManagerNiri:
			niriPath := filepath.Join(os.Getenv("HOME"), ".config", "niri", "config.kdl")
			niriExists := false
			if _, err := os.Stat(niriPath); err == nil {
				niriExists = true
			}
			configs = append(configs, ExistingConfigInfo{
				ConfigType: "Niri",
				Path:       niriPath,
				Exists:     niriExists,
			})
		case deps.WindowManagerMango:
			mangoConfPath := filepath.Join(os.Getenv("HOME"), ".config", "mango", "config.conf")
			mangoMainPath := filepath.Join(os.Getenv("HOME"), ".config", "mango", "mango.conf")
			mangoPath := mangoConfPath
			mangoExists := false
			if _, err := os.Stat(mangoConfPath); err == nil {
				mangoExists = true
			} else if _, err := os.Stat(mangoMainPath); err == nil {
				mangoPath = mangoMainPath
				mangoExists = true
			}
			configs = append(configs, ExistingConfigInfo{
				ConfigType: "Mango",
				Path:       mangoPath,
				Exists:     mangoExists,
			})
		default:
			hyprlandLuaPath := filepath.Join(os.Getenv("HOME"), ".config", "hypr", "hyprland.lua")
			hyprlandConfPath := filepath.Join(os.Getenv("HOME"), ".config", "hypr", "hyprland.conf")
			hyprlandPath := hyprlandLuaPath
			hyprlandExists := false
			if _, err := os.Stat(hyprlandLuaPath); err == nil {
				hyprlandExists = true
			} else if _, err := os.Stat(hyprlandConfPath); err == nil {
				hyprlandPath = hyprlandConfPath
				hyprlandExists = true
			}
			configs = append(configs, ExistingConfigInfo{
				ConfigType: "Hyprland",
				Path:       hyprlandPath,
				Exists:     hyprlandExists,
			})
		}

		if m.osInfo != nil && m.osInfo.Distribution.ID == "gentoo" {
			if m.selectedTerminal == 0 {
				kittyPath := filepath.Join(os.Getenv("HOME"), ".config", "kitty", "kitty.conf")
				kittyExists := false
				if _, err := os.Stat(kittyPath); err == nil {
					kittyExists = true
				}
				configs = append(configs, ExistingConfigInfo{
					ConfigType: "Kitty",
					Path:       kittyPath,
					Exists:     kittyExists,
				})
			} else {
				alacrittyPath := filepath.Join(os.Getenv("HOME"), ".config", "alacritty", "alacritty.toml")
				alacrittyExists := false
				if _, err := os.Stat(alacrittyPath); err == nil {
					alacrittyExists = true
				}
				configs = append(configs, ExistingConfigInfo{
					ConfigType: "Alacritty",
					Path:       alacrittyPath,
					Exists:     alacrittyExists,
				})
			}
		} else {
			switch m.selectedTerminal {
			case 0:
				ghosttyPath := filepath.Join(os.Getenv("HOME"), ".config", "ghostty", "config")
				ghosttyExists := false
				if _, err := os.Stat(ghosttyPath); err == nil {
					ghosttyExists = true
				}
				configs = append(configs, ExistingConfigInfo{
					ConfigType: "Ghostty",
					Path:       ghosttyPath,
					Exists:     ghosttyExists,
				})
			case 1:
				kittyPath := filepath.Join(os.Getenv("HOME"), ".config", "kitty", "kitty.conf")
				kittyExists := false
				if _, err := os.Stat(kittyPath); err == nil {
					kittyExists = true
				}
				configs = append(configs, ExistingConfigInfo{
					ConfigType: "Kitty",
					Path:       kittyPath,
					Exists:     kittyExists,
				})
			default:
				alacrittyPath := filepath.Join(os.Getenv("HOME"), ".config", "alacritty", "alacritty.toml")
				alacrittyExists := false
				if _, err := os.Stat(alacrittyPath); err == nil {
					alacrittyExists = true
				}
				configs = append(configs, ExistingConfigInfo{
					ConfigType: "Alacritty",
					Path:       alacrittyPath,
					Exists:     alacrittyExists,
				})
			}
		}

		return configCheckResult{
			configs: configs,
			error:   nil,
		}
	}
}
