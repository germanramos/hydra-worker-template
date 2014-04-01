package main

import (
	"os"
)

func main() {
	if len(os.Args) < 3 {
		panic("Invalid number of arguments, you need to add at least the arguments for the server address and the service name")
	}
	serverAddr := os.Args[1]  // e.g. "tcp://localhost:5555"
	serviceName := os.Args[2] // e.g. pong
	verbose := len(os.Args) >= 4 && os.Args[3] == "-v"

	// New Worker connected to Hydra Load Balancer
	worker := NewWorker(serverAddr, serviceName, verbose)

	for reply := [][]byte{}; ; {
		request := worker.Recv(reply)
		if len(request) == 0 {
			break
		}
		// You should code your logic here
		reply = request
	}
}
