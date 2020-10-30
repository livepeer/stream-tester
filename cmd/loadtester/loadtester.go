// Load tester is a tool to do load testing
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/joy4/format"
	"github.com/livepeer/stream-tester/apis/livepeer"
	"github.com/livepeer/stream-tester/internal/metrics"
	"github.com/livepeer/stream-tester/internal/testers"
	"github.com/livepeer/stream-tester/internal/utils"
	"github.com/livepeer/stream-tester/model"
	"github.com/peterbourgon/ff/v2"
)

func init() {
	format.RegisterAll()
}

func main() {
	flag.Set("logtostderr", "true")
	vFlag := flag.Lookup("v")

	fs := flag.NewFlagSet("loadteter", flag.ExitOnError)

	verbosity := fs.String("v", "", "Log verbosity.  {4|5|6}")
	version := fs.Bool("version", false, "Print out the version")

	// startDelay := fs.Duration("start-delay", 0*time.Second, "time delay before start")
	streamDuration := fs.Duration("stream-dur", 0, "How long to stream (0 to stream whole file)")

	profiles := fs.Uint("profiles", 2, "number of transcoded profiles should be in output")
	sim := fs.Uint("sim", 1, "Number of simulteneous streams to stream")
	delayBetweenGroups := fs.Duration("delay-between-streams", 2*time.Second, "Delay between starting group of streams")
	fileArg := fs.String("file", "bbb_sunflower_1080p_30fps_normal_t02.mp4", "File to stream")
	apiToken := fs.String("api-token", "", "Token of the Livepeer API to be used")
	apiServer := fs.String("api-server", "livepeer.com", "Server of the Livepeer API to be used")
	httpIngest := fs.Bool("http-ingest", false, "Use Livepeer HTTP HLS ingest")
	ignoreNoCodecError := fs.Bool("ignore-no-codec-error", true, "Do not stop streaming if segment without codec's info downloaded")

	_ = fs.String("config", "", "config file (optional)")

	ff.Parse(fs, os.Args[1:],
		ff.WithConfigFileFlag("config"),
		ff.WithConfigFileParser(ff.PlainParser),
		ff.WithEnvVarPrefix("LOADTESTER"),
	)
	flag.CommandLine.Parse(nil)
	vFlag.Value.Set(*verbosity)

	hostName, _ := os.Hostname()
	fmt.Println("Loadtester version: " + model.Version)
	fmt.Printf("Compiler version: %s %s\n", runtime.Compiler, runtime.Version())
	fmt.Printf("Hostname %s OS %s IPs %v\n", hostName, runtime.GOOS, utils.GetIPs())
	fmt.Printf("Production: %v\n", model.Production)

	if *version {
		return
	}
	metrics.InitCensus(hostName, model.Version)
	testers.IgnoreNoCodecError = *ignoreNoCodecError
	testers.IgnoreGaps = true
	testers.IgnoreTimeDrift = true
	testers.StartDelayBetweenGroups = *delayBetweenGroups

	if *fileArg == "" {
		fmt.Println("Should provide -file argumnet")
		os.Exit(1)
	}
	var err error
	var fileName string

	if *profiles == 0 {
		fmt.Println("Number of profiles couldn't be set to zero")
		os.Exit(1)
	}

	if *apiToken == "" {
		glog.Fatalf("-api-token should be specified")
	}

	if fileName, err = utils.GetFile(*fileArg); err != nil {
		if err == utils.ErrNotFound {
			fmt.Printf("File %s not found\n", *fileArg)
		} else {
			fmt.Printf("Error getting file %s: %v\n", *fileArg, err)
		}
		os.Exit(1)
	}
	fmt.Printf("Streaming video file '%s'\n", fileName)
	exit := func(exitCode int, fn, fa string, err error) {
		if fn != fa {
			os.Remove(fn)
		}
		if err != nil {
			fmt.Printf("Error: %v", err)
		}
		os.Exit(exitCode)
	}

	model.ProfilesNum = int(*profiles)

	lapi := livepeer.NewLivepeer(*apiToken, *apiServer, nil)
	lapi.Init()
	glog.Infof("Choosen server: %s", lapi.GetServer())
	ingests, err := lapi.Ingest(false)
	if err != nil {
		exit(255, fileName, *fileArg, err)
	}
	glog.Infof("Got ingests: %+v", ingests)
	broadcasters, err := lapi.Broadcasters()
	if err != nil {
		exit(255, fileName, *fileArg, err)
	}
	glog.Infof("Got broadcasters: %+v", broadcasters)

	gctx, gcancel := context.WithCancel(context.Background()) // to be used as global parent context, in the future

	// sr := testers.NewHTTPLoadTester(gctx, gcancel, lapi, 0)
	var sr model.Streamer
	if !*httpIngest {
		sr = testers.NewStreamer(gctx, gcancel, false, true, nil, lapi)
	} else {
		sr = testers.NewHTTPLoadTester(gctx, gcancel, lapi, 0)
	}
	baseManifesID, err := sr.StartStreams(fileName, "", "1935", "", "443", *sim, 1, *streamDuration, false, true, true, 2, 5*time.Second, 0)
	if err != nil {
		exit(255, fileName, *fileArg, err)
	}
	glog.Infof("Base manfiest id: %s", baseManifesID)
	exitc := make(chan os.Signal, 1)
	signal.Notify(exitc, syscall.SIGINT, syscall.SIGTERM, os.Interrupt, os.Kill)
	go func() {
		<-exitc
		fmt.Println("Got Ctrl-C, cancelling")
		gcancel()
		sr.Cancel()
		time.Sleep(2 * time.Second)
		stats, _ := sr.Stats("")
		fmt.Println(stats.FormatForConsole())
		if fileName != *fileArg {
			os.Remove(fileName)
		}
	}()
	glog.Infof("Waiting for test to complete")
	<-sr.Done()
	<-gctx.Done()
	time.Sleep(1 * time.Second)
	fmt.Println("========= Stats: =========")
	stats, _ := sr.Stats("")
	fmt.Println(stats.FormatForConsole())
	fmt.Println(stats.FormatErrorsForConsole())
	exit(model.ExitCode, fileName, *fileArg, err)
}
