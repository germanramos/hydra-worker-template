package main

import (
	"os"
	"log"
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
		log.Print("***********************Enter FOR")
		request := worker.Recv(reply)
		log.Print("*********************After recv")
		if len(request) == 0 {
			break
		}
		// You should code your logic here
		log.Print("********************REPLY=REQUEST")
		reply = request
	}
}
