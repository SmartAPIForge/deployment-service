package ssh

import (
	"fmt"
	"strings"

	"deployment-service/internal/model"

	"golang.org/x/crypto/ssh"
)

// GetContainerIDByPort returns the container ID that is using the specified port on the server
func GetContainerIDByPort(server model.Server, port int) (string, error) {
	config := &ssh.ClientConfig{
		User: server.User,
		Auth: []ssh.AuthMethod{
			ssh.Password(server.Password),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	addr := fmt.Sprintf("%s:%d", server.IP, server.Port)
	client, err := ssh.Dial("tcp", addr, config)
	if err != nil {
		return "", fmt.Errorf("could not connect to the server: %v", err)
	}
	defer func(client *ssh.Client) {
		err := client.Close()
		if err != nil {
			fmt.Printf("error with closing ssh connection: %v\n", err)
		}
	}(client)

	// Command to find container ID by port
	cmd := fmt.Sprintf("docker ps --format '{{.ID}}' --filter 'publish=%d'", port)

	output, err := runCommand(client, cmd)
	if err != nil {
		return "", fmt.Errorf("error getting container ID: %v", err)
	}

	containerID := strings.TrimSpace(output)
	if containerID == "" {
		return "", fmt.Errorf("no container found using port %d", port)
	}

	return containerID, nil
}
