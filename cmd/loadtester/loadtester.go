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

func init() {
	format.RegisterAll()
}

func main() {
	var cliFlags = &cliArguments{}

	flag.Set("logtostderr", "true")
	vFlag := flag.Lookup("v")

	fs := flag.NewFlagSet("loadtester", flag.ExitOnError)

	fs.IntVar(&cliFlags.Verbosity, "v", 3, "Log verbosity.  {4|5|6}")
	fs.BoolVar(&cliFlags.Version, "version", false, "Print out the version")
	fs.BoolVar(&cliFlags.MistMode, "mist", false, "Mist mode (remove session query)")
	fs.BoolVar(&cliFlags.HTTPIngest, "http-ingest", false, "Use Livepeer HTTP HLS ingest")

	// startDelay := fs.Duration("start-delay", 0*time.Second, "time delay before start")
	fs.DurationVar(&cliFlags.StreamDuration, "stream-dur", 0, "How long to stream each stream (0 to stream whole file)")
	fs.DurationVar(&cliFlags.TestDuration, "test-dur", 0, "How long to run overall test")
	fs.DurationVar(&cliFlags.WaitForTargetDuration, "wait-for-target", 30*time.Second, "How long to wait for a new stream to appear before giving up")
	fs.DurationVar(&cliFlags.StartDelayDuration, "delay-between-streams", 2*time.Second, "Delay between starting group of streams")

	// profiles := fs.Uint("profiles", 2, "number of transcoded profiles should be in output")
	fs.UintVar(&cliFlags.Simultaneous, "sim", 1, "Number of simulteneous streams to stream")
	fs.StringVar(&cliFlags.Filename, "file", "bbb_sunflower_1080p_30fps_normal_t02.mp4", "File to stream")
	fs.StringVar(&cliFlags.APIToken, "api-token", "", "Token of the Livepeer API to be used")
	fs.StringVar(&cliFlags.APIServer, "api-server", "livepeer.com", "Server of the Livepeer API to be used")
	fs.StringVar(&cliFlags.RTMPTemplate, "rtmp-template", "", "Template of RTMP ingest URL")
	fs.StringVar(&cliFlags.HLSTemplate, "hls-template", "", "Template of HLS playback URL")
	// ignoreNoCodecError := fs.Bool("ignore-no-codec-error", true, "Do not stop streaming if segment without codec's info downloaded")

	_ = fs.String("config", "", "config file (optional)")

	ff.Parse(fs, os.Args[1:],
		ff.WithConfigFileFlag("config"),
		ff.WithConfigFileParser(ff.PlainParser),
		ff.WithEnvVarPrefix("LOADTESTER"),
	)
	flag.CommandLine.Parse(nil)
	vFlag.Value.Set(fmt.Sprintf("%d", cliFlags.Verbosity))

	hostName, _ := os.Hostname()
	fmt.Println("Loadtester version: " + model.Version)
	fmt.Printf("Compiler version: %s %s\n", runtime.Compiler, runtime.Version())
	fmt.Printf("Hostname %s OS %s IPs %v\n", hostName, runtime.GOOS, utils.GetIPs())
	fmt.Printf("Production: %v\n", model.Production)

	if cliFlags.Version {
		return
	}
	metrics.InitCensus(hostName, model.Version, "loadtester")
	// testers.IgnoreNoCodecError = *ignoreNoCodecError
	testers.IgnoreNoCodecError = true
	testers.IgnoreGaps = true
	testers.IgnoreTimeDrift = true
	testers.StartDelayBetweenGroups = cliFlags.StartDelayDuration
	model.ProfilesNum = 0

	if cliFlags.Filename == "" {
		glog.Fatal("missing --file parameter")
	}
	var err error
	var fileName string

	// if *profiles == 0 {
	// 	fmt.Println("Number of profiles couldn't be set to zero")
	// 	os.Exit(1)
	// }
	// model.ProfilesNum = int(*profiles)

	if cliFlags.TestDuration == 0 {
		glog.Fatalf("-test-dur should be specified")
	}
	if cliFlags.APIToken != "" && (cliFlags.RTMPTemplate != "") {
		glog.Infof("notice: overriding ingest URL returned by %s with %s", cliFlags.APIServer, cliFlags.RTMPTemplate)
	}
	if cliFlags.APIToken != "" && (cliFlags.HLSTemplate != "") {
		glog.Infof("notice: overriding playback URL returned by %s with %s", cliFlags.APIServer, cliFlags.HLSTemplate)
	}
	if cliFlags.APIToken == "" && cliFlags.RTMPTemplate == "" {
		glog.Fatalf("-api-token or -rtmp-template should be specified")
	}
	if cliFlags.RTMPTemplate != "" && cliFlags.HTTPIngest {
		glog.Fatal("-http-ingest can't be specified together with -rtmp-template")
	}

	if fileName, err = utils.GetFile(cliFlags.Filename, strings.ReplaceAll(hostName, ".", "_")); err != nil {
		if err == utils.ErrNotFound {
			glog.Fatalf("file %s not found\n", cliFlags.Filename)
		} else {
			glog.Fatalf("error getting file %s: %v\n", cliFlags.Filename, err)
		}
	}
	glog.Infof("streaming video file %q", fileName)
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
			glog.Errorf("Error: %v\n", err)
		}
		os.Exit(exitCode)
	}
	gctx, gcancel := context.WithCancel(context.Background()) // to be used as global parent context, in the future
	var streamStarter model.StreamStarter
	var id int
	var mu sync.Mutex
	if cliFlags.APIToken != "" {
		lapi = livepeer.NewLivepeer(cliFlags.APIToken, cliFlags.APIServer, nil)
		lapi.Init()
		glog.Infof("Choosen server: %s", lapi.GetServer())
		ingests, err := lapi.Ingest(false)
		if err != nil {
			exit(255, fileName, cliFlags.Filename, err)
		}
		glog.Infof("Got ingests: %+v", ingests)
		broadcasters, err := lapi.Broadcasters()
		if err != nil {
			exit(255, fileName, cliFlags.Filename, err)
		}
		glog.Infof("Got broadcasters: %+v", broadcasters)
		httpIngestURLTemplates := make([]string, 0, len(broadcasters))
		for _, b := range broadcasters {
			httpIngestURLTemplates = append(httpIngestURLTemplates, fmt.Sprintf("%s/live/%%s", b))
		}
		if cliFlags.HTTPIngest && len(broadcasters) == 0 {
			exit(254, fileName, cliFlags.Filename, errors.New("Empty list of broadcasters"))
		} else if !cliFlags.HTTPIngest && len(ingests) == 0 {
			exit(254, fileName, cliFlags.Filename, errors.New("Empty list of ingests"))
		}
		streamStarter = func(ctx context.Context, sourceFileName string, waitForTarget, timeToStream time.Duration) (model.OneTestStream, error) {
			mu.Lock()
			manifestID := fmt.Sprintf("%s_%d", hostName, id)
			id++
			mu.Unlock()
			stream, err := lapi.CreateStreamEx(manifestID, false, nil)
			if err != nil {
				glog.Errorf("Error creating stream using Livepeer API: %v", err)
				return nil, err
			}
			glog.V(model.VERBOSE).Infof("Create Livepeer stream id=%s streamKey=%s playbackId=%s", stream.ID, stream.StreamKey, stream.PlaybackID)
			createdAPIStreams = append(createdAPIStreams, stream.ID)
			if cliFlags.HTTPIngest {
				httpIngestURLTemplate := httpIngestURLTemplates[id%len(httpIngestURLTemplates)]
				httpIngestURL := fmt.Sprintf(httpIngestURLTemplate, stream.ID)
				glog.V(model.SHORT).Infof("HTTP ingest: %s", httpIngestURL)

				up := testers.NewHTTPStreamer(ctx, false, hostName)
				go up.StartUpload(sourceFileName, httpIngestURL, manifestID, 0, waitForTarget, timeToStream, 0)
				return up, nil
			}
			var rtmpURL string
			if cliFlags.RTMPTemplate != "" {
				rtmpURL = fmt.Sprintf(cliFlags.RTMPTemplate, stream.StreamKey)
			} else {
				rtmpURL = fmt.Sprintf("%s/%s", ingests[0].Ingest, stream.StreamKey)
			}

			var mediaURL string
			if cliFlags.HLSTemplate != "" {
				mediaURL = fmt.Sprintf(cliFlags.HLSTemplate, stream.PlaybackID)
			} else {
				mediaURL = fmt.Sprintf("%s/%s/index.m3u8", ingests[0].Playback, stream.PlaybackID)
			}
			glog.V(model.SHORT).Infof("RTMP: %s", rtmpURL)
			glog.V(model.SHORT).Infof("MEDIA: %s", mediaURL)
			sr2 := testers.NewStreamer2(ctx, testers.Streamer2Options{MistMode: cliFlags.MistMode})
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
			rtmpURL := fmt.Sprintf(cliFlags.RTMPTemplate, manifestID)
			mediaURL := fmt.Sprintf(cliFlags.HLSTemplate, manifestID)
			glog.V(model.SHORT).Infof("RTMP: %s", rtmpURL)
			glog.V(model.SHORT).Infof("MEDIA: %s", mediaURL)
			if err := utils.WaitForTCP(waitForTarget, rtmpURL); err != nil {
				// glog.Error(err)
				return nil, err
			}
			sr2 := testers.NewStreamer2(ctx, testers.Streamer2Options{MistMode: cliFlags.MistMode})
			go sr2.StartStreaming(sourceFileName, rtmpURL, mediaURL, waitForTarget, timeToStream)
			return sr2, nil
		}
	}

	loadTester := testers.NewLoadTester(gctx, streamStarter, cliFlags.StartDelayDuration)
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
	}(fileName, cliFlags.Filename)

	err = loadTester.Start(fileName, cliFlags.WaitForTargetDuration, cliFlags.StreamDuration, cliFlags.TestDuration, int(cliFlags.Simultaneous))
	if err != nil {
		glog.Errorf("Error starting test: %v", err)
		exit(255, fileName, cliFlags.Filename, err)
	}
	<-loadTester.Done()
	glog.Infof("Testing finished")
	cleanup(fileName, cliFlags.Filename)
	stats, _ := loadTester.Stats()
	glog.Info(stats.FormatForConsole())
	// exit(255, fileName, cliFlags.Filename, err)

	/*
		lapi := livepeer.NewLivepeer(cliFlags.APIToken, cliFlags.APIServer, nil)
		lapi.Init()
		glog.Infof("Choosen server: %s", lapi.GetServer())
		ingests, err := lapi.Ingest(false)
		if err != nil {
			exit(255, fileName, cliFlags.Filename, err)
		}
		glog.Infof("Got ingests: %+v", ingests)
		broadcasters, err := lapi.Broadcasters()
		if err != nil {
			exit(255, fileName, cliFlags.Filename, err)
		}
		glog.Infof("Got broadcasters: %+v", broadcasters)

		gctx, gcancel := context.WithCancel(context.Background()) // to be used as global parent context, in the future

		// sr := testers.NewHTTPLoadTester(gctx, gcancel, lapi, 0)
		var sr model.Streamer
		if !cliFlags.HTTPIngest {
			sr = testers.NewStreamer(gctx, gcancel, false, true, nil, lapi)
		} else {
			sr = testers.NewHTTPLoadTester(gctx, gcancel, lapi, 0)
		}
		baseManifesID, err := sr.StartStreams(fileName, "", "1935", "", "443", *sim, 1, cliFlags.StreamDuration, false, true, true, 2, 5*time.Second, 0)
		if err != nil {
			exit(255, fileName, cliFlags.Filename, err)
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
			if fileName != cliFlags.Filename {
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
		exit(model.ExitCode, fileName, cliFlags.Filename, err)
	*/
}

func randName() string {
	x := make([]byte, 10, 10)
	for i := 0; i < len(x); i++ {
		x[i] = byte(rand.Uint32())
	}
	return fmt.Sprintf("%x", x)
}
