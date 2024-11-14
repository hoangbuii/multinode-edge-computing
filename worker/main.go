// Worker
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

// STAGE: connected, idle
var STAGE = "idle"

func joinSwarm(token string, masterIP string) error {
	// Define the shell script command
	cmd := exec.Command("bash", "-c", "docker swarm join --token "+token+" "+masterIP+":2377")

	// Run the command and capture the output
	_, err := cmd.Output()

	return err
}

func leaveSwarm() error {
	cmd := exec.Command("bash", "-c", "docker swarm leave")
	_, err := cmd.Output()
	return err
}

func checkDockerVersion() (string, error) {
	// Define the shell script command
	cmd := exec.Command("bash", "-c", "docker --version")

	// Run the command and capture the output
	output, err := cmd.Output()
	token := string(output)
	return token, err
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

func RetrieveIPFromMasterID(masterID string, redisAdd string, port int) (string, error) {
	redisAddress := redisAdd + ":" + strconv.Itoa(port)
	var ctx = context.Background()
	var rdb = redis.NewClient(&redis.Options{
		Addr: redisAddress, // Change to your Redis server address
		DB:   0,            // Default DB
	})
	// Retrieve the IP associated with the MasterID key
	IP, err := rdb.Get(ctx, masterID).Result()
	if err == redis.Nil {
		// Key does not exist
		return "", fmt.Errorf("no IP found for masterID: %s", masterID)
	} else if err != nil {
		// Other Redis error
		return "", fmt.Errorf("error retrieving IP for masterID %s: %v", masterID, err)
	}

	return IP, nil
}

func RetrieveWorkerIDFromMasterID(masterID string, redisAdd string, port int) (string, error) {
	redisAddress := redisAdd + ":" + strconv.Itoa(port)
	var ctx = context.Background()
	var rdb = redis.NewClient(&redis.Options{
		Addr: redisAddress, // Change to your Redis server address
		DB:   1,            // Default DB
	})
	// Retrieve the message associated with the WorkerID key
	workerID, err := rdb.Get(ctx, masterID).Result()
	if err == redis.Nil {
		// Key does not exist
		return "", fmt.Errorf("no WorkerID found for masterID: %s", masterID)
	} else if err != nil {
		// Other Redis error
		return "", fmt.Errorf("error retrieving WorkerID for masterID %s: %v", masterID, err)
	}

	return workerID, nil
}

// StoreMessagesToRedis stores the collected IP-message pairs in Redis
func StoreMessagesToRedis(messages map[string]string, redisAdd string, port int) error {
	redisAddress := redisAdd + ":" + strconv.Itoa(port)
	var ctx = context.Background()
	var rdb = redis.NewClient(&redis.Options{
		Addr: redisAddress, // Change to your Redis server address
		DB:   0,            // Default DB
	})
	for message, ip := range messages {
		// Use IP as the key and message as the value
		err := rdb.Set(ctx, message, ip, 0).Err()
		if err != nil {
			return fmt.Errorf("failed to store message for IP %s: %v", ip, err)
		}
		fmt.Printf("Stored IP: %s, Message: %s in Redis\n", ip, message)
	}
	return nil
}

// StoreMessagesToRedis stores the collected IP-message pairs in Redis
func StoreWorkerIDToRedis(workerID string, masterID string, redisAdd string, port int) error {
	redisAddress := redisAdd + ":" + strconv.Itoa(port)
	var ctx = context.Background()
	var rdb = redis.NewClient(&redis.Options{
		Addr: redisAddress, // Change to your Redis server address
		DB:   1,            // Default DB
	})
	err := rdb.Set(ctx, masterID, workerID, 0).Err()
	if err != nil {
		return fmt.Errorf("failed to store message for IP %s: %v", workerID, err)
	}
	fmt.Printf("Stored Worker ID: %s, MasterID: %s in Redis\n", workerID, masterID)
	return nil
}

func scanner(port int) map[string]string {
	// Create a map to store unique messages with IP as the key and message as the value
	uniqueMessages := make(map[string]string)

	// Listen on UDP port 9090
	addr := net.UDPAddr{
		Port: port,
		IP:   net.IPv4zero, // Listen on all interfaces
	}

	// Create a UDP connection
	conn, err := net.ListenUDP("udp", &addr)
	if err != nil {
		fmt.Println("Error creating UDP listener:", err)
		return uniqueMessages
	}
	defer conn.Close()

	fmt.Printf("Listening for broadcast messages on port %d for 11 seconds...\n", port)

	buffer := make([]byte, 1024)
	timeout := time.After(11 * time.Second)

	for {
		select {
		case <-timeout:
			fmt.Println("Receiver timed out after 11 seconds.")
			return uniqueMessages
		default:
			// Set a read deadline to allow checking the timeout regularly
			conn.SetReadDeadline(time.Now().Add(1 * time.Second))

			// Read the incoming broadcast message
			n, remoteAddr, err := conn.ReadFromUDP(buffer)
			if err != nil {
				// Ignore timeout errors caused by SetReadDeadline
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					continue
				}
				fmt.Println("Error reading message:", err)
				continue
			}

			// Store unique IP and message
			message := string(buffer[:n])
			if _, exists := uniqueMessages[message]; !exists {
				uniqueMessages[message] = remoteAddr.IP.String()
			}
		}
	}
}

func startTCPConnection(serverIP string, tcpPort int, command string) {
	conn, err := net.Dial("tcp", serverIP+":"+strconv.Itoa(tcpPort))
	if err != nil {
		STAGE = "idle"
		fmt.Println("Error connecting to TCP server:", err)
		return
	}
	defer conn.Close()

	fmt.Println("Connected to server:", serverIP)

	// Continuously send messages to server until the user types "exit"

	message := command

	if message == "exit" {
		STAGE = "idle"
		fmt.Println("Closing connection to server")
		return
	}

	// Send message to server
	_, err = conn.Write([]byte(message + "\n"))
	if err != nil {
		fmt.Println("Error sending message to server:", err)
		return
	}

	// Start a goroutine to listen for messages from the server
	for {
		// Read response from server

		response, err := bufio.NewReader(conn).ReadString('\n')
		if err != nil {
			STAGE = "idle"
			fmt.Println("Server closed the connection")
			return
		}

		fmt.Println("Received from server:", strings.TrimSpace(response))
		if strings.HasPrefix(response, "SWMTKN") {
			err := joinSwarm(strings.TrimSuffix(response, "\n"), serverIP)
			if err != nil {
				fmt.Println("error i docker1")
				return
			}
		}
		STAGE = "connected"
	}

}

func printHelp() {
	fmt.Println("Multi-Node Edge Computing Platform: Worker Node \n")
	fmt.Println("Usage:\n")
	fmt.Println("\tecwcli <command> [arguments]\n")
	fmt.Println("The commands are:\n")
	fmt.Println("\tversion\t\tprint platform version")
	fmt.Println("\tscan\t\tscan for master node")
	fmt.Println("\tconnect\t\tget connection from master node")
	fmt.Println("\tstart\t\tstart platform")
	fmt.Println("\tstop\t\tstop platform")
}

func printVersion() {
	fmt.Println("Multi-Node Edge Computing Platform: Worker Node")
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
		fmt.Println("Error:", err)
	} else {
		fmt.Println("Redis server is reachable.")
	}
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: ecw <start|stop>")
		return
	}
	command := os.Args[1]
	pidFile := "/tmp/test.pid"

	switch command {
	case "version":
		printVersion()
	case "start":
		for {
			fmt.Println("Stage:" + STAGE)
			if STAGE == "idle" {
				uniqueMessages := scanner(9090)
				err := writePidToFile(pidFile)
				if err != nil {
					fmt.Println("Error writing PID to file:", err)
					return
				}
				for mID, mIP := range uniqueMessages {
					fmt.Printf("MasterID: %s, IP: %s\n", mID, mIP)
					workerID, err := RetrieveWorkerIDFromMasterID(mID, "localhost", 6379)
					if err != nil {
						fmt.Println("Error:", err)
					} else {
						startTCPConnection(mIP, 8080, workerID)
					}
				}

				if err := StoreMessagesToRedis(uniqueMessages, "localhost", 6379); err != nil {
					fmt.Println("Error storing messages to Redis:", err)
				} else {
					fmt.Println("All messages stored in Redis successfully.")
				}
			}

			time.Sleep(1 * time.Minute)
		}
	case "scan":
		uniqueMessages := scanner(9090)
		err := writePidToFile(pidFile)
		if err != nil {
			fmt.Println("Error writing PID to file:", err)
			return
		}
		for mID, mIP := range uniqueMessages {
			fmt.Printf("MasterID: %s, IP: %s\n", mID, mIP)
		}

		if err := StoreMessagesToRedis(uniqueMessages, "localhost", 6379); err != nil {
			fmt.Println("Error storing messages to Redis:", err)
		}
	case "get":
		if len(os.Args) < 3 {
			fmt.Println("Missing masterID name")
			return
		}
		mID := os.Args[2]
		IP, err := RetrieveIPFromMasterID(mID, "localhost", 6379)
		if err != nil {
			fmt.Println("Error:", err)
		} else {
			fmt.Printf("IP for Master %s: %s\n", mID, IP)
		}
		workerID, err := RetrieveWorkerIDFromMasterID(mID, "localhost", 6379)
		if err != nil {
			fmt.Println("Error:", err)
		} else {
			fmt.Printf("WokerID for Master %s: %s\n", mID, workerID)
		}
	case "connect":
		if len(os.Args) < 3 {
			fmt.Println("Missing masterID name")
			return
		}
		mID := os.Args[2]
		workerID := "worker_" + GenerateID(10)
		if err := StoreWorkerIDToRedis(workerID, mID, "localhost", 6379); err != nil {
			fmt.Println("Error storing messages to Redis:", err)
		} else {
			fmt.Println("Set Master " + mID + " to known Node")
		}
		// IP, err := RetrieveIPFromMasterID(mID, "localhost", 6379)
		// err = writePidToFile(pidFile)
		// if err != nil {
		// 	fmt.Println("Error writing PID to file:", err)
		// 	return
		// }
		// if err != nil {
		// 	fmt.Println("Error:", err)
		// } else {
		// 	fmt.Printf("IP for Master %s: %s\n", mID, IP)
		// 	startTCPConnection(IP, 8080, workerID)
		// }
	case "stop":
		// Stop the process from the PID file
		err := leaveSwarm()
		if err != nil {
			fmt.Println("Error to leave Swarm: ", err)
		}
		err = stopProcessFromPidFile(pidFile)
		if err != nil {
			fmt.Println("Error stopping process:", err)
		}
	case "help":
		printHelp()
	default:
		fmt.Println("Unknown command.\n")
		printHelp()
	}
}
