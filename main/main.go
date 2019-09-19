package main

import (
	"api-cache/server"
	"log"
	"os"
	"strconv"
)

func main() {
	// Use port from command line or default to 8080.
	port := int(8080)
	if len (os.Args) > 1 {
		portStr := os.Args[1]
		var e error
		port, e = strconv.Atoi(portStr)
		if e != nil {
			log.Panicf("Invalid port on cmdline %s", portStr)
		}
	}
	// Load API token from env.
	apiToken := os.Getenv("GITHUB_API_TOKEN")
	// Create and run the server.
	s := server.NewServer(uint32(port), apiToken)
	s.Run()
}
