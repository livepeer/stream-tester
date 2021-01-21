// Load tester is a tool to do load testing
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
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
	streamDuration := fs.Duration("stream-dur", 0, "How long to stream each stream (0 to stream whole file)")
	testDuration := fs.Duration("test-dur", 0, "How long to run overall test")

	// profiles := fs.Uint("profiles", 2, "number of transcoded profiles should be in output")
	sim := fs.Uint("sim", 1, "Number of simulteneous streams to stream")
	delayBetweenGroups := fs.Duration("delay-between-streams", 2*time.Second, "Delay between starting group of streams")
	fileArg := fs.String("file", "bbb_sunflower_1080p_30fps_normal_t02.mp4", "File to stream")
	apiToken := fs.String("api-token", "", "Token of the Livepeer API to be used")
	apiServer := fs.String("api-server", "livepeer.com", "Server of the Livepeer API to be used")
	httpIngest := fs.Bool("http-ingest", false, "Use Livepeer HTTP HLS ingest")
	rtmpTemplate := fs.String("rtmp-template", "", "Template of RTMP ingest URL")
	hlsTemplate := fs.String("hls-template", "", "Template of HLS playback URL")
	// ignoreNoCodecError := fs.Bool("ignore-no-codec-error", true, "Do not stop streaming if segment without codec's info downloaded")

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
	metrics.InitCensus(hostName, model.Version, "loadtester")
	// testers.IgnoreNoCodecError = *ignoreNoCodecError
	testers.IgnoreNoCodecError = true
	testers.IgnoreGaps = true
	testers.IgnoreTimeDrift = true
	testers.StartDelayBetweenGroups = *delayBetweenGroups
	model.ProfilesNum = 0

	if *fileArg == "" {
		fmt.Println("Should provide -file argumnet")
		os.Exit(1)
	}
	var err error
	var fileName string

	// if *profiles == 0 {
	// 	fmt.Println("Number of profiles couldn't be set to zero")
	// 	os.Exit(1)
	// }
	// model.ProfilesNum = int(*profiles)

	if *testDuration == 0 {
		glog.Fatalf("-test-dur should be specified")
	}
	if *rtmpTemplate == "" && *hlsTemplate != "" {
		glog.Fatal("Should also specify -rtmp-template")
	}
	if *apiToken != "" && (*rtmpTemplate != "") {
		glog.Infof("notice: overriding ingest URL returned by %s with %s", *apiServer, *rtmpTemplate)
	}
	if *apiToken == "" && *rtmpTemplate == "" {
		glog.Fatalf("-api-token or -rtmp-template should be specified")
	}
	if *rtmpTemplate != "" && *httpIngest {
		glog.Fatal("-http-ingest can't be specified together with -rtmp-template")
	}

	if fileName, err = utils.GetFile(*fileArg, strings.ReplaceAll(hostName, ".", "_")); err != nil {
		if err == utils.ErrNotFound {
			fmt.Printf("File %s not found\n", *fileArg)
		} else {
			fmt.Printf("Error getting file %s: %v\n", *fileArg, err)
		}
		os.Exit(1)
	}
	fmt.Printf("Streaming video file '%s'\n", fileName)
	var lapi *livepeer.API
	var createdAPIStreams []string
	cleanup := func(fn, fa string) {
		if fn != fa {
			os.Remove(fn)
		}
		if lapi != nil && len(createdAPIStreams) > 0 {
			for _, sid := range createdAPIStreams {
				lapi.DeleteStream(sid)
			}
		}
	}
	exit := func(exitCode int, fn, fa string, err error) {
		cleanup(fn, fa)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
		}
		os.Exit(exitCode)
	}
	gctx, gcancel := context.WithCancel(context.Background()) // to be used as global parent context, in the future
	var streamStarter model.StreamStarter
	var id int
	var mu sync.Mutex
	if *apiToken != "" {
		lapi = livepeer.NewLivepeer(*apiToken, *apiServer, nil)
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
		httpIngestURLTemplates := make([]string, 0, len(broadcasters))
		for _, b := range broadcasters {
			httpIngestURLTemplates = append(httpIngestURLTemplates, fmt.Sprintf("%s/live/%%s", b))
		}
		if *httpIngest && len(broadcasters) == 0 {
			exit(254, fileName, *fileArg, errors.New("Empty list of broadcasters"))
		} else if !*httpIngest && len(ingests) == 0 {
			exit(254, fileName, *fileArg, errors.New("Empty list of ingests"))
		}
		streamStarter = func(ctx context.Context, sourceFileName string, waitForTarget, timeToStream time.Duration) (model.OneTestStream, error) {
			mu.Lock()
			manifestID := fmt.Sprintf("%s_%d", hostName, id)
			id++
			mu.Unlock()
			stream, err := lapi.CreateStreamEx(manifestID)
			if err != nil {
				glog.Errorf("Error creating stream using Livepeer API: %v", err)
				return nil, err
			}
			glog.V(model.VERBOSE).Infof("Create Livepeer stream id=%s streamKey=%s playbackId=%s", stream.ID, stream.StreamKey, stream.PlaybackID)
			createdAPIStreams = append(createdAPIStreams, stream.ID)
			if *httpIngest {
				httpIngestURLTemplate := httpIngestURLTemplates[id%len(httpIngestURLTemplates)]
				httpIngestURL := fmt.Sprintf(httpIngestURLTemplate, stream.ID)
				glog.V(model.SHORT).Infof("HTTP ingest: %s", httpIngestURL)

				up := testers.NewHTTPStreamer(ctx, false, hostName)
				go up.StartUpload(sourceFileName, httpIngestURL, manifestID, 0, waitForTarget, timeToStream, 0)
				return up, nil
			}
			var rtmpURL string
			if *rtmpTemplate != "" {
				rtmpURL = fmt.Sprintf(*rtmpTemplate, stream.StreamKey)
			} else {
				rtmpURL = fmt.Sprintf("%s/%s", ingests[0].Ingest, stream.StreamKey)
			}
			mediaURL := fmt.Sprintf("%s/%s/index.m3u8", ingests[0].Playback, stream.PlaybackID)
			glog.V(model.SHORT).Infof("RTMP: %s", rtmpURL)
			glog.V(model.SHORT).Infof("MEDIA: %s", mediaURL)
			sr2 := testers.NewStreamer2(ctx, false, false, false, false, false)
			go sr2.StartStreaming(sourceFileName, rtmpURL, mediaURL, waitForTarget, timeToStream)
			go func() {
				<-sr2.Done()
				lapi.DeleteStream(stream.ID)
			}()
			return sr2, nil
		}
	} else {
		baseName := strings.ReplaceAll(hostName, ".", "_") + "_" + randName()
		streamStarter = func(ctx context.Context, sourceFileName string, waitForTarget, timeToStream time.Duration) (model.OneTestStream, error) {
			mu.Lock()
			manifestID := fmt.Sprintf("%s_%d", baseName, id)
			id++
			mu.Unlock()
			rtmpURL := fmt.Sprintf(*rtmpTemplate, manifestID)
			mediaURL := fmt.Sprintf(*hlsTemplate, manifestID)
			glog.V(model.SHORT).Infof("RTMP: %s", rtmpURL)
			glog.V(model.SHORT).Infof("MEDIA: %s", mediaURL)
			if err := utils.WaitForTCP(waitForTarget, rtmpURL); err != nil {
				// glog.Error(err)
				return nil, err
			}
			sr2 := testers.NewStreamer2(ctx, false, false, false, false, false)
			go sr2.StartStreaming(sourceFileName, rtmpURL, mediaURL, waitForTarget, timeToStream)
			return sr2, nil
		}
	}

	loadTester := testers.NewLoadTester(gctx, streamStarter, *delayBetweenGroups)
	exitc := make(chan os.Signal, 1)
	signal.Notify(exitc, syscall.SIGINT, syscall.SIGTERM, os.Interrupt, os.Kill)
	go func(fn, fa string) {
		<-exitc
		fmt.Println("Got Ctrl-C, cancelling")
		gcancel()
		cleanup(fn, fa)
		time.Sleep(2 * time.Second)
		stats, _ := loadTester.Stats()
		fmt.Println(stats.FormatForConsole())
		// exit(0, fn, fa, nil)
	}(fileName, *fileArg)

	err = loadTester.Start(fileName, 30*time.Second, *streamDuration, *testDuration, int(*sim))
	if err != nil {
		glog.Errorf("Error starting test: %v", err)
		exit(255, fileName, *fileArg, err)
	}
	<-loadTester.Done()
	glog.Infof("Testing finished")
	cleanup(fileName, *fileArg)
	stats, _ := loadTester.Stats()
	glog.Info(stats.FormatForConsole())
	// exit(255, fileName, *fileArg, err)

	/*
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
	*/
}

func randName() string {
	x := make([]byte, 10, 10)
	for i := 0; i < len(x); i++ {
		x[i] = byte(rand.Uint32())
	}
	return fmt.Sprintf("%x", x)
}
