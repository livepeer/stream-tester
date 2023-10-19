package main

import (
	"fmt"
	"os"

	"github.com/livepeer/stream-tester/cmd/webrtc-load-tester/roles"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "First program argument must be the role in the load test. Allowed values are: orchestrator, streamer, player")
		os.Exit(1)
	}

	role := os.Args[1]
	switch role {
	case "orchestrator":
		roles.Orchestrator()
	case "player":
		roles.Player()
	case "streamer":
		roles.Streamer()
	case "screenshot":
		roles.Screenshot()
	default:
		fmt.Fprintln(os.Stderr, "Role must be one of: orchestrator, streamer, player")
		fmt.Fprintf(os.Stderr, "Got: %s\n", role)
		os.Exit(1)
	}
}
