package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/matteso1/sentinel/internal/server"
)

func main() {
	// Parse flags
	port := flag.Int("port", 9092, "Server port")
	dataDir := flag.String("data", "./data", "Data directory")
	flag.Parse()

	// Create config
	config := server.DefaultServerConfig()
	config.Port = *port
	config.DataDir = *dataDir

	// Create server
	srv, err := server.NewServer(config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create server: %v\n", err)
		os.Exit(1)
	}

	// Handle shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		fmt.Println("\nShutting down...")
		srv.Stop()
		os.Exit(0)
	}()

	// Start server
	fmt.Printf("Starting Sentinel server on port %d (data: %s)\n", *port, *dataDir)
	if err := srv.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "Server error: %v\n", err)
		os.Exit(1)
	}
}
