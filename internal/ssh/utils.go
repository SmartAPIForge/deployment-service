package ssh

import (
	"bytes"
	"fmt"

	"golang.org/x/crypto/ssh"
)

// runCommand executes a command over SSH and returns the output
func runCommand(client *ssh.Client, cmd string) (string, error) {
	session, err := client.NewSession()
	if err != nil {
		return "", fmt.Errorf("error with creating SSH-connection: %v", err)
	}
	defer func(session *ssh.Session) {
		err := session.Close()
		if err != nil {
			// Ignore close errors
		}
	}(session)

	var stdout bytes.Buffer
	session.Stdout = &stdout

	if err := session.Run(cmd); err != nil {
		return "", fmt.Errorf("error with command processing: %v", err)
	}

	return stdout.String(), nil
}
