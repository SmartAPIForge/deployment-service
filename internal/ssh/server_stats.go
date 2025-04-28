package ssh

import (
	"deployment-service/internal/model"
	"fmt"
	"strconv"
	"strings"

	"golang.org/x/crypto/ssh"
)

type ServerStats struct {
	CPUUsagePercent  float64
	MemoryTotalBytes uint64
	MemoryUsedBytes  uint64
	MemoryPercent    float64
}

func GetOptimizedServer(servers []model.Server) (model.Server, error) {
	if len(servers) == 0 {
		return model.Server{}, fmt.Errorf("no servers provided")
	}

	var optimizedServer model.Server
	minMemoryUsage := 100.0

	for _, server := range servers {
		stats, err := GetServerStats(server)
		if err != nil {
			return model.Server{}, err
		}

		if stats.MemoryPercent < minMemoryUsage {
			minMemoryUsage = stats.MemoryPercent
			optimizedServer = server
		}
	}

	if optimizedServer == (model.Server{}) {
		return model.Server{}, fmt.Errorf("no optimized server found")
	}

	return optimizedServer, nil
}

func GetServerStats(server model.Server) (*ServerStats, error) {

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
		return nil, fmt.Errorf("could not connect to the server: %v", err)
	}
	defer func(client *ssh.Client) {
		err := client.Close()
		if err != nil {
			fmt.Printf("error with closing ssh connection: %v\n", err)
		}
	}(client)

	stats := &ServerStats{}

	if err := getCPUUsage(client, stats); err != nil {
		return nil, err
	}

	if err := getMemoryInfo(client, stats); err != nil {
		return nil, err
	}

	return stats, nil
}

func getCPUUsage(client *ssh.Client, stats *ServerStats) error {
	cmd := "top -bn1 | grep '%Cpu' | awk '{print $2}'"

	output, err := runCommand(client, cmd)
	if err != nil {
		return fmt.Errorf("error with getting CPU data: %v", err)
	}

	cpuUsage, err := strconv.ParseFloat(strings.TrimSpace(output), 64)
	if err != nil {
		return fmt.Errorf("error with parsing CPU data: %v", err)
	}

	stats.CPUUsagePercent = cpuUsage
	return nil
}

func getMemoryInfo(client *ssh.Client, stats *ServerStats) error {
	cmd := "free -b | grep Mem:"

	output, err := runCommand(client, cmd)
	if err != nil {
		return fmt.Errorf("error with getting memory data: %v", err)
	}

	fields := strings.Fields(output)
	if len(fields) < 3 {
		return fmt.Errorf("unhandled command format output: free")
	}

	total, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return fmt.Errorf("parse error total memory: %v", err)
	}

	used, err := strconv.ParseUint(fields[2], 10, 64)
	if err != nil {
		return fmt.Errorf("parse error used memory: %v", err)
	}

	stats.MemoryTotalBytes = total
	stats.MemoryUsedBytes = used
	stats.MemoryPercent = float64(used) / float64(total) * 100.0

	return nil
}
