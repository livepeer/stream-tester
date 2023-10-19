package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"time"

	"github.com/livepeer/stream-tester/internal/utils"
	"github.com/livepeer/stream-tester/model"
	"github.com/peterbourgon/ff/v2"
)

type cliArguments struct {
	Verbosity    int
	Simultaneous uint
	Version      bool
	MistMode     bool
	HTTPIngest   bool
	RTMPTemplate string
	HLSTemplate  string
	APIServer    string
	APIToken     string
	Filename     string

	StreamDuration        time.Duration
	TestDuration          time.Duration
	WaitForTargetDuration time.Duration
	StartDelayDuration    time.Duration
}

func main2() {
	var cliFlags = &cliArguments{}

	flag.Set("logtostderr", "true")
	vFlag := flag.Lookup("v")

	fs := flag.NewFlagSet("webrtc-load-tester", flag.ExitOnError)

	fs.IntVar(&cliFlags.Verbosity, "v", 3, "Log verbosity.  {4|5|6}")
	fs.BoolVar(&cliFlags.Version, "version", false, "Print out the version")
	fs.BoolVar(&cliFlags.MistMode, "mist", false, "Mist mode (remove session query)")
	fs.BoolVar(&cliFlags.HTTPIngest, "http-ingest", false, "Use Livepeer HTTP HLS ingest")

	fs.DurationVar(&cliFlags.StreamDuration, "stream-dur", 0, "How long to stream each stream (0 to stream whole file)")
	fs.DurationVar(&cliFlags.TestDuration, "test-dur", 0, "How long to run overall test")
	fs.DurationVar(&cliFlags.WaitForTargetDuration, "wait-for-target", 30*time.Second, "How long to wait for a new stream to appear before giving up")
	fs.DurationVar(&cliFlags.StartDelayDuration, "delay-between-streams", 2*time.Second, "Delay between starting group of streams")

	fs.UintVar(&cliFlags.Simultaneous, "sim", 1, "Number of simulteneous streams to stream")
	fs.StringVar(&cliFlags.Filename, "file", "bbb_sunflower_1080p_30fps_normal_t02.mp4", "File to stream")
	fs.StringVar(&cliFlags.APIToken, "api-token", "", "Token of the Livepeer API to be used")
	fs.StringVar(&cliFlags.APIServer, "api-server", "livepeer.com", "Server of the Livepeer API to be used")
	fs.StringVar(&cliFlags.RTMPTemplate, "rtmp-template", "", "Template of RTMP ingest URL")
	fs.StringVar(&cliFlags.HLSTemplate, "hls-template", "", "Template of HLS playback URL")

	_ = fs.String("config", "", "config file (optional)")

	ff.Parse(fs, os.Args[1:],
		ff.WithConfigFileFlag("config"),
		ff.WithConfigFileParser(ff.PlainParser),
		ff.WithEnvVarPrefix("LT_WEBRTC"),
	)
	flag.CommandLine.Parse(nil)
	vFlag.Value.Set(fmt.Sprintf("%d", cliFlags.Verbosity))

	hostName, _ := os.Hostname()
	fmt.Println("WebRTC Load Tester version: " + model.Version)
	fmt.Printf("Compiler version: %s %s\n", runtime.Compiler, runtime.Version())
	fmt.Printf("Hostname %s OS %s IPs %v\n", hostName, runtime.GOOS, utils.GetIPs())
	fmt.Printf("Production: %v\n", model.Production)

}

func randName() string {
	x := make([]byte, 10, 10)
	for i := 0; i < len(x); i++ {
		x[i] = byte(rand.Uint32())
	}
	return fmt.Sprintf("%x", x)
}
