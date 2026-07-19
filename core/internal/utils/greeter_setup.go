package utils

import (
	"bufio"
	"fmt"
	"os/exec"
	"strings"
)

// RunDmsGreeterInstall delegates greeter setup to the standalone dms-greeter
// binary, which owns greetd configuration since the greeter moved out of DMS.
func RunDmsGreeterInstall(sudoPassword string, logFunc func(string)) error {
	binary, err := exec.LookPath("dms-greeter")
	if err != nil {
		return fmt.Errorf("dms-greeter binary not found; install the dms-greeter package and run 'dms-greeter install'")
	}

	var cmd *exec.Cmd
	if sudoPassword == "" {
		cmd = exec.Command(binary, "install", "--yes")
	} else {
		cmd = exec.Command("sudo", "-S", "-p", "", binary, "install", "--yes")
		cmd.Stdin = strings.NewReader(sudoPassword + "\n")
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	cmd.Stderr = cmd.Stdout

	if err := cmd.Start(); err != nil {
		return err
	}
	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		logFunc(scanner.Text())
	}
	return cmd.Wait()
}
