package main

import (
	"deployment-service/internal/model"
	"deployment-service/internal/ssh"
	"fmt"
	"log"
	"os"
)

func main() {
	fmt.Println(os.Args)

	if len(os.Args) < 5 {
		log.Fatal("Usage: go run main.go <ip> <port> <user> <password> <container-port>")
	}

	fmt.Println(os.Args)

	server := model.Server{
		IP:       os.Args[1],
		Port:     uint32(mustParseInt(os.Args[2])),
		User:     os.Args[3],
		Password: os.Args[4],
	}

	containerPort := mustParseInt(os.Args[5])

	containerID, err := ssh.GetContainerIDByPort(server, containerPort)
	if err != nil {
		log.Fatalf("Error getting container ID: %v", err)
	}

	fmt.Printf("Container ID using port %d: %s\n", containerPort, containerID)
}

func mustParseInt(s string) int {
	var i int
	_, err := fmt.Sscanf(s, "%d", &i)
	if err != nil {
		log.Fatalf("Error parsing port number: %v", err)
	}
	return i
}
