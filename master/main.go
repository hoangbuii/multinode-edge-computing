// Master
package main

import (
	"bufio"
	"context"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/go-redis/redis/v8"
)

var masterID = "master_"

func initSwarm() {
	cmd := exec.Command("bash", "-c", "docker swarm init")
	_, err := cmd.Output()
	if err == nil {
		fmt.Println("INFO: Initialize Docker Swarm")
	} else {
		fmt.Println("WARN: Docker Swarm has already been initialized")
	}
}

func getJoinToken() (string, error) {
	// Define the shell script command
	cmd := exec.Command("bash", "-c", "docker swarm join-token worker --quiet")

	// Run the command and capture the output
	output, err := cmd.Output()
	token := string(output)
	return token, err
}

func checkDockerVersion() (string, error) {
	// Define the shell script command
	cmd := exec.Command("bash", "-c", "docker --version")

	// Run the command and capture the output
	output, err := cmd.Output()
	token := string(output)
	return token, err
}

func removeDownNode() error {

	cmd := exec.Command("bash", "-c", "docker node ls | grep 'Down' | awk '{print $1}'")
	haveDownNode, err := cmd.Output()
	if string(haveDownNode) == "" {
		return nil
	}
	cmd = exec.Command("bash", "-c", "docker node rm $(docker node ls | grep 'Down' | awk '{print $1}')")

	// Run the command and capture the output
	_, err = cmd.Output()

	return err

}

func listNode() (string, error) {
	// Define the shell script command
	cmd := exec.Command("bash", "-c", "docker node ls --format '{{.ID}}'")

	// Run the command and capture the output
	output, err := cmd.Output()
	token := string(output)
	return token, err
}

func listService() (string, error) {
	// Define the shell script command
	cmd := exec.Command("bash", "-c", "docker service ls --format '{{.ID}}'")

	// Run the command and capture the output
	output, err := cmd.Output()
	token := string(output)
	return token, err
}

func scaleAllServiceTo(replicas int) error {
	command := "for i in $(docker service ls --format '{{.ID}}'); do docker service scale $i=" + strconv.Itoa(replicas) + "; done"
	cmd := exec.Command("bash", "-c", command)
	// fmt.Println("Service has been scaled to " + strconv.Itoa(replicas))
	// Run the command and capture the output
	_, err := cmd.Output()

	return err
}

func writePidToFile(filename string) error {
	pid := os.Getpid()
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.WriteString(strconv.Itoa(pid))
	if err != nil {
		return err
	}

	return nil
}

func stopProcessFromPidFile(filename string) error {
	// Read the PID from the file
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to open pid file: %v", err)
	}
	defer file.Close()

	var pid int
	_, err = fmt.Fscanf(file, "%d", &pid)
	if err != nil {
		return fmt.Errorf("failed to read pid from file: %v", err)
	}

	// Find the process with that PID
	process, err := os.FindProcess(pid)
	if err != nil {
		return fmt.Errorf("failed to find process with PID %d: %v", pid, err)
	}

	// Send a termination signal to the process
	err = process.Signal(syscall.SIGTERM) // or syscall.SIGKILL for forceful termination
	if err != nil {
		return fmt.Errorf("failed to send signal to process: %v", err)
	}

	// Wait a moment to ensure termination
	time.Sleep(2 * time.Second)

	// Remove the PID file content
	err = os.Truncate("/tmp/test.pid", 0)
	if err != nil {
		return fmt.Errorf("failed to truncate pid file: %v", err)
	}

	return nil
}

func GenerateID(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyz0123456789"
	rand.Seed(time.Now().UnixNano())
	var sb strings.Builder
	for i := 0; i < length; i++ {
		sb.WriteByte(charset[rand.Intn(len(charset))])
	}
	return sb.String()
}

// PingRedis checks if the Redis server is reachable at the specified address and port.
func PingRedis(redisAdd string, port int) error {
	redisAddress := redisAdd + ":" + strconv.Itoa(port)
	ctx := context.Background()

	// Create a Redis client
	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddress, // Redis server address
		DB:   0,            // Default DB
	})

	// Perform the ping
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		return fmt.Errorf("could not connect to Redis at %s: %v", redisAddress, err)
	}

	//fmt.Printf("Successfully connected to Redis at %s\n", redisAddress)
	return nil
}

func StoreWorkerIDToRedis(mID string, wID string, redisAdd string, port int) error {
	redisAddress := redisAdd + ":" + strconv.Itoa(port)
	var ctx = context.Background()
	var rdb = redis.NewClient(&redis.Options{
		Addr: redisAddress, // Change to your Redis server address
		DB:   0,            // Default DB
	})
	err := rdb.Set(ctx, wID, mID, 0).Err()
	if err != nil {
		return fmt.Errorf("failed to store WorkerID for MasterID %s: %v", wID, err)
	}
	fmt.Printf("INFO: Stored WorkerID: %s, MasterID: %s in Redis\n", wID, mID)

	return nil
}

func getBroadcastAddress(ifaceName string) (string, error) {
	// Find the network interface by name
	iface, err := net.InterfaceByName(ifaceName)
	if err != nil {
		return "", fmt.Errorf("interface %s not found: %v", ifaceName, err)
	}

	// Get the list of addresses for this interface
	addrs, err := iface.Addrs()
	if err != nil {
		return "", fmt.Errorf("cannot get addresses for interface %s: %v", ifaceName, err)
	}

	// Loop over the addresses to find the first IPv4 address
	for _, addr := range addrs {
		ipNet, ok := addr.(*net.IPNet)
		if ok && ipNet.IP.To4() != nil {
			// Calculate the broadcast address by setting the host bits to 1
			ip := ipNet.IP.To4()
			mask := ipNet.Mask
			broadcast := net.IPv4(
				ip[0]|^mask[0],
				ip[1]|^mask[1],
				ip[2]|^mask[2],
				ip[3]|^mask[3],
			)
			return broadcast.String(), nil
		}
	}

	return "", fmt.Errorf("no IPv4 address found for interface %s", ifaceName)
}

func broadcaster(ifaceName string, message string) {
	// Define the broadcast address and port.
	broadcastAddr, _ := getBroadcastAddress(ifaceName)
	broadcastAddr = broadcastAddr + ":9090"

	// Create a UDP connection.
	conn, err := net.Dial("udp", broadcastAddr)
	if err != nil {
		fmt.Println("ERROR: Error creating connection:", err)
		return
	}
	defer conn.Close()

	// Enable broadcasting
	if err := conn.(*net.UDPConn).SetWriteBuffer(1024); err != nil {
		fmt.Println("ERROR: Error setting broadcast:", err)
		return
	}
	fmt.Println("INFO: Server broadcast via port 9090")
	// Broadcast the message every 10 seconds
	for {
		_, err = conn.Write([]byte(message))
		if err != nil {
			fmt.Println("ERROR: Error sending message:", err)
			return
		}
		//fmt.Println("Message broadcasted successfully!")

		// Wait for 10 seconds before broadcasting again
		time.Sleep(10 * time.Second)
	}
}

// handleTCPConnection handles an individual TCP connection with a client
func handleTCPConnection(conn net.Conn) {
	defer conn.Close()
	fmt.Println("INFO: Connected to worker:", conn.RemoteAddr())

	// Continuously read messages from client until the client closes the connection
	for {
		message, err := bufio.NewReader(conn).ReadString('\n')
		if err != nil {
			fmt.Println("INFO: Worker disconnected:", conn.RemoteAddr())
			// err = removeDownNode()
			// if err != nil {
			// 	fmt.Println("error i docker")
			// }
			return
		}
		if strings.HasPrefix(message, "worker_") {
			fmt.Printf("INFO: %s has been joined to cluster\n", strings.TrimSuffix(message, "\n"))
		}
		// fmt.Println("Received from client:", message)

		if err := StoreWorkerIDToRedis(masterID, strings.TrimSuffix(message, "\n"), "localhost", 6379); err != nil {
			fmt.Println("ERROR: Error storing messages to Redis:", err)
		} else {
			// fmt.Println("All messages stored in Redis successfully.")
		}
		// Respond to client
		response, err := getJoinToken()
		if err != nil {
			fmt.Println("ERROR: Failed to get join token")
			return
		}
		_, err = conn.Write([]byte(response))
		if err != nil {
			fmt.Println("ERROR: Error sending message to client:", err)
			return
		}
	}
}

// startTCPServer starts a TCP server to listen for client connections
func startTCPServer(tcpPort int) {
	listener, err := net.Listen("tcp", ":"+strconv.Itoa(tcpPort))
	if err != nil {
		fmt.Println("ERROR: Error starting TCP server:", err)
		return
	}
	defer listener.Close()

	fmt.Println("INFO: TCP server listening on port", tcpPort)
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("ERROR: Error accepting connection:", err)
			continue
		}
		go handleTCPConnection(conn)
	}
}

// Calculate the broadcast address
func calculateBroadcast(ip net.IP, mask net.IPMask) net.IP {
	ip = ip.To4()
	broadcast := make(net.IP, len(ip))
	for i := 0; i < len(ip); i++ {
		broadcast[i] = ip[i] | ^mask[i]
	}
	return broadcast
}

// Get the default gateway (Unix-based systems)
func getDefaultGateway(iface string) (string, error) {
	out, err := exec.Command("ip", "route", "show", "default", "0.0.0.0/0").Output()
	if err != nil {
		return "", err
	}
	parts := strings.Fields(string(out))
	if len(parts) > 2 && parts[0] == "default" && parts[1] == "via" {
		return parts[2], nil
	}
	return "", fmt.Errorf("default gateway not found")
}

// Get the DNS servers from /etc/resolv.conf (Unix-based systems)
func getDNSServers() ([]string, error) {
	out, err := exec.Command("cat", "/etc/resolv.conf").Output()
	if err != nil {
		return nil, err
	}

	var dnsServers []string
	lines := strings.Split(string(out), "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, "nameserver") {
			parts := strings.Fields(line)
			if len(parts) > 1 {
				dnsServers = append(dnsServers, parts[1])
			}
		}
	}
	return dnsServers, nil
}

func showNetworkConfiguration() {
	// Get all network interfaces
	interfaces, err := net.Interfaces()
	if err != nil {
		fmt.Println("Error fetching interfaces:", err)
		return
	}

	for _, iface := range interfaces {
		// Skip down or loopback interfaces
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}

		fmt.Println("Interface Name:", iface.Name)

		addrs, err := iface.Addrs()
		if err != nil {
			fmt.Println("Error fetching addresses:", err)
			continue
		}

		for _, addr := range addrs {
			ipNet, ok := addr.(*net.IPNet)
			if !ok || ipNet.IP.IsLoopback() {
				continue
			}

			if ipNet.IP.To4() != nil {
				fmt.Println("  IP Address:", ipNet.IP.String())
				fmt.Println("  Subnet Mask:", ipNet.Mask.String())

				// Calculate and print broadcast address
				broadcast := calculateBroadcast(ipNet.IP, ipNet.Mask)
				fmt.Println("  Broadcast Address:", broadcast.String())
			}
		}

		// Fetch default gateway
		gateway, err := getDefaultGateway(iface.Name)
		if err == nil {
			fmt.Println("  Default Gateway:", gateway)
		} else {
			fmt.Println("  Default Gateway: Not found")
		}

		// Fetch DNS servers
		dnsServers, err := getDNSServers()
		if err == nil {
			fmt.Println("  DNS Servers:", dnsServers)
		} else {
			fmt.Println("  DNS Servers: Not found")
		}

		fmt.Println()
	}
}

func scaleService() {
	for {
		err := removeDownNode()
		if err != nil {
			fmt.Println("ERROR: Failed to remode Down node")
		}
		swarmNode, err := listNode()
		if err != nil {
			fmt.Println("EROR: Failed to list Node")
		}
		lines := strings.Split(swarmNode, "\n")
		numberOfSwarmNode := len(lines) - 1
		// fmt.Printf("Number of Swarm Node: %d\n", numberOfSwarmNode)
		time.Sleep(1 * time.Minute)
		// swarmService, err := listService()
		// if err != nil {
		// 	fmt.Println("Fail to list Service")
		// }
		// lines = strings.Split(swarmService, "\n")
		// numberOfSwarmService := len(lines) - 1
		// fmt.Printf("Number of Swarm Node: %d\n", numberOfSwarmService)
		err = scaleAllServiceTo(numberOfSwarmNode)
		if err != nil {
			fmt.Println("ERROR: Failed to scale node")
		}
	}

}

func printHelp() {
	fmt.Println("Multi-Node Edge Computing Platform: Master Node \n")
	fmt.Println("Usage:\n")
	fmt.Println("\tecmcli <command> [arguments]\n")
	fmt.Println("The commands are:\n")
	fmt.Println("\tversion\t\tprint platform version")
	fmt.Println("\tnetwork\t\tshow all network interface")
	fmt.Println("\tstart\t\tstart platform in specific network")
	fmt.Println("\tstop\t\tstop platform")
}

func printVersion() {
	fmt.Println("Multi-Node Edge Computing Platform: Master Node")
	fmt.Println("Version: 1.0")
	dockerVersion, err := checkDockerVersion()
	if err != nil {
		fmt.Println("Can't found Docker!")
		return
	} else {
		fmt.Print(dockerVersion)
	}
	err = PingRedis("localhost", 6379)
	if err != nil {
		fmt.Println("ERROR:", err)
	} else {
		fmt.Println("Redis server is reachable.")
	}
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Missing command")
		printHelp()
		return
	}
	command := os.Args[1]
	pidFile := "/tmp/test.pid"
	//broadcast_iface := "ens33"

	switch command {
	case "version":
		printVersion()
	case "init":
		initSwarm()
	case "network":
		showNetworkConfiguration()
	case "start":
		_, err := checkDockerVersion()
		if err != nil {
			fmt.Println("FATAL: Can't found Docker!")
			return
		}
		err = PingRedis("localhost", 6379)
		if err != nil {
			fmt.Println("WARN: Unable to connect to redis")
		}
		if len(os.Args) < 3 {
			fmt.Println("Missing interface name")
			return
		}
		broadcast_iface := os.Args[2]
		err = writePidToFile(pidFile)
		if err != nil {
			fmt.Println("FATAL: Error writing PID to file:", err)
			return
		}

		masterID = "master_" + GenerateID(10)
		fmt.Println("INFO: Start with " + masterID)
		go broadcaster(broadcast_iface, masterID)
		go startTCPServer(8080)
		go scaleService()
		for {

		}
	case "help":
		printHelp()
	case "stop":
		// Stop the process from the PID file
		err := stopProcessFromPidFile(pidFile)
		if err != nil {
			fmt.Println("ERROR: Error stopping process:", err)
		}
	default:
		fmt.Println("Unknown command.\n")
		printHelp()
	}
}
