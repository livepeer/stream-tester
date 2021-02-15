// a tool to run continuous streams to help monitor the platform
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
	"github.com/livepeer/stream-tester/internal/server"
	"github.com/livepeer/stream-tester/internal/testers"
	"github.com/livepeer/stream-tester/internal/utils"
	"github.com/livepeer/stream-tester/model"
	"github.com/peterbourgon/ff/v2"
)

func init() {
	format.RegisterAll()

	// override glog's default -logtostderr flag
	err := flag.Set("logtostderr", "true")
	if err != nil {
		panic(err)
	}

	model.ProfilesNum = 1
}

func main() {
	var err error

	fs := flag.NewFlagSet("stream-monitor", flag.ExitOnError)

	apiServer := fs.String("api-server", "livepeer.com", "Server of the Livepeer API to be used")
	apiToken := fs.String("api-token", "", "Token of the Livepeer API to be used")
	bind := fs.String("bind", "0.0.0.0:9090", "Address to bind metric server to")
	fileArg := fs.String("file", "bbb_sunflower_1080p_30fps_normal_t02.mp4", "File to stream")
	streamDuration := fs.Duration("stream-duration", 0, "How long to stream (0 to stream whole file)")
	verbosity := fs.String("v", "", "Log verbosity.  {4|5|6}")
	version := fs.Bool("version", false, "Print out the version")

	_ = fs.String("config", "", "config file (optional)")
	_ = ff.Parse(
		fs,
		os.Args[1:],
		ff.WithConfigFileFlag("config"),
		ff.WithConfigFileParser(ff.PlainParser),
		ff.WithEnvVarPrefix("STREAM_MONITOR"),
	)

	// configure glog
	vFlag := flag.Lookup("v")
	_ = vFlag.Value.Set(*verbosity)

	// supress errors in log output from glog
	err = flag.CommandLine.Parse(nil)
	if err != nil {
		panic(err)
	}

	hostName, _ := os.Hostname()

	// Print startup
	fmt.Println("StreamMonitor version: " + model.Version)
	fmt.Printf("Compiler version: %s %s\n", runtime.Compiler, runtime.Version())
	fmt.Printf("Hostname %s OS %s IPs %v\n", hostName, runtime.GOOS, utils.GetIPs())
	fmt.Printf("Production: %v\n", model.Production)

	gctx, gcancel := context.WithCancel(context.Background()) // to be used as global parent context, in the future
	defer gcancel()

	metrics.InitCensus(hostName, model.Version, "streammonitor")

	metricServer := server.NewMetricsServer()
	go metricServer.Start(gctx, *bind)

	if *version {
		return
	}

	if _, err := os.Stat(*fileArg); os.IsNotExist(err) {
		fmt.Printf("File '%s' does not exists", *fileArg)
		os.Exit(1)
	}

	if *apiToken == "" {
		glog.Fatalf("-api-token should be specified")
	}

	streamDone := make(chan bool, 1)
	done := make(chan bool, 1)
	sigs := make(chan os.Signal)
	lapi := livepeer.NewLivepeer(*apiToken, *apiServer, nil)

	var stream livepeer.CreateStreamResp

	startStream := func() {
		lapi.Init()
		glog.Infof("Choosen server: %s", lapi.GetServer())
		ingests, err := lapi.Ingest(false)
		if err != nil {
			panic(err)
		}
		glog.Infof("Got ingests: %+v", ingests)
		broadcasters, err := lapi.Broadcasters()
		if err != nil {
			panic(err)
		}
		glog.Infof("Got broadcasters: %+v", broadcasters)
		streamName := fmt.Sprintf("%s_%s", hostName, time.Now().Format(time.RFC3339Nano))
		stream, err := lapi.CreateStreamEx(streamName, nil)
		if err != nil {
			panic(err)
		}

		up := testers.NewHTTPStreamer(gctx, true, "baseManifestID")
		up.StartUpload(*fileArg, broadcasters[0]+"/live/"+stream.ID, stream.ID, 0, 0, *streamDuration, 0)

		stats, err := up.StatsOld()
		if err != nil {
			panic(err)
		}

		glog.Infof("Deleting stream %s", stream.ID)
		err = lapi.DeleteStream(stream.ID)
		if err != nil {
			glog.Errorf("Error deleting stream %s: %v", stream.ID, err)
		}

		fmt.Println("========= Stats: =========")
		fmt.Println(stats.FormatForConsole())
		fmt.Println(stats.FormatErrorsForConsole())

		streamDone <- true
	}

	// setup signal trapping
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, os.Interrupt)
	go func() {
		<-sigs
		fmt.Println("Got Ctrl-C, cancelling")

		err = lapi.DeleteStream(stream.ID)
		if err != nil {
			glog.Errorf("Error deleting stream %s: %v", stream.ID, err)
		}
		fmt.Println("done")

		done <- true
	}()

	// run stream cycles until interupt or panic
	for {
		go startStream()

		select {
		case <-streamDone:
			fmt.Println("Stream Cycle Complete")
		case <-done:
			os.Exit(model.ExitCode)
		}
	}
}
