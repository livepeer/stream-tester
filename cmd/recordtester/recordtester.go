// Record tester is a tool to test Livepeer API's recording functionality
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"strings"
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

const useForceURL = false

func init() {
	format.RegisterAll()
}

var standardProfiles = []livepeer.Profile{
	{
		Name:    "240p0",
		Fps:     0,
		Bitrate: 250000,
		Width:   426,
		Height:  240,
		Gop:     "2.0",
	},
	{
		Name:    "360p0",
		Fps:     0,
		Bitrate: 800000,
		Width:   640,
		Height:  360,
		Gop:     "2.0",
	},
	{
		Name:    "480p0",
		Fps:     0,
		Bitrate: 1600000,
		Width:   854,
		Height:  480,
		Gop:     "2.0",
	},
	{
		Name:    "720p0",
		Fps:     0,
		Bitrate: 3000000,
		Width:   1280,
		Height:  720,
		Gop:     "2.0",
	},
}

func main() {
	flag.Set("logtostderr", "true")
	vFlag := flag.Lookup("v")

	fs := flag.NewFlagSet("loadteter", flag.ExitOnError)

	verbosity := fs.String("v", "", "Log verbosity.  {4|5|6}")
	version := fs.Bool("version", false, "Print out the version")

	// startDelay := fs.Duration("start-delay", 0*time.Second, "time delay before start")
	testDuration := fs.Duration("test-dur", 0, "How long to run overall test")
	apiToken := fs.String("api-token", "", "Token of the Livepeer API to be used")
	apiServer := fs.String("api-server", "livepeer.com", "Server of the Livepeer API to be used")
	httpIngest := fs.Bool("http-ingest", false, "Use Livepeer HTTP HLS ingest")
	fileArg := fs.String("file", "bbb_sunflower_1080p_30fps_normal_t02.mp4", "File to stream")
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
	testers.IgnoreNoCodecError = true
	testers.IgnoreGaps = true
	testers.IgnoreTimeDrift = true
	testers.StartDelayBetweenGroups = 0
	model.ProfilesNum = 0

	if *fileArg == "" {
		fmt.Println("Should provide -file argumnet")
		os.Exit(1)
	}
	var err error
	var fileName string

	gctx, gcancel := context.WithCancel(context.Background()) // to be used as global parent context, in the future
	defer gcancel()
	// es := checkDown(gctx, "https://fra-cdn.livepeer.monster/recordings/474a6bc4-94fd-469d-a8c4-ec94bceb0323/index.m3u8", *testDuration)
	// os.Exit(es)
	// return

	// if *profiles == 0 {
	// 	fmt.Println("Number of profiles couldn't be set to zero")
	// 	os.Exit(1)
	// }
	// model.ProfilesNum = int(*profiles)

	if *testDuration == 0 {
		glog.Fatalf("-test-dur should be specified")
	}
	if *apiToken == "" {
		glog.Fatalf("-api-token should be specified")
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
			// for _, sid := range createdAPIStreams {
			// lapi.DeleteStream(sid)
			// }
		}
	}
	exit := func(exitCode int, fn, fa string, err error) {
		cleanup(fn, fa)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
		}
		os.Exit(exitCode)
	}
	lapi = livepeer.NewLivepeer2(*apiToken, *apiServer, nil, 8*time.Second)
	lapi.Init()
	glog.Infof("Choosen server: %s", lapi.GetServer())

	/*
		sessionsx, err := lapi.GetSessions("1f770f0a-9177-49bd-a848-023abee7c09b")
		if err != nil {
			glog.Errorf("Error getting sessions for stream id=%s err=%v", ".ID", err)
			exit(252, fileName, *fileArg, err)
		}
		glog.Infof("Sessions: %+v", sessionsx)
	*/

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
	glog.Infof("All cool!")
	streamName := fmt.Sprintf("%s_%s", hostName, time.Now().Format("2006-01-02T15:04:05Z07:00"))
	stream, err := lapi.CreateStreamEx(streamName, nil, standardProfiles...)
	if err != nil {
		glog.Errorf("Error creating stream using Livepeer API: %v", err)
		exit(253, fileName, *fileArg, err)
	}
	createdAPIStreams = append(createdAPIStreams, stream.ID)
	glog.V(model.VERBOSE).Infof("Created Livepeer stream id=%s streamKey=%s playbackId=%s name=%s", stream.ID, stream.StreamKey, stream.PlaybackID, streamName)
	// glog.Infof("Waiting 5 second for stream info to propagate to the Postgres replica")
	// time.Sleep(5 * time.Second)
	rtmpURL := fmt.Sprintf("%s/%s", ingests[0].Ingest, stream.StreamKey)
	// rtmpURL = fmt.Sprintf("%s/%s", ingests[0].Ingest, stream.ID)

	mediaURL := fmt.Sprintf("%s/%s/index.m3u8", ingests[0].Playback, stream.PlaybackID)
	glog.V(model.SHORT).Infof("RTMP: %s", rtmpURL)
	glog.V(model.SHORT).Infof("MEDIA: %s", mediaURL)

	sr2 := testers.NewStreamer2(gctx, false, false, false, false, false)
	go sr2.StartStreaming(fileName, rtmpURL, mediaURL, 30*time.Second, *testDuration)

	// uploader := testers.NewRtmpStreamer(gctx, rtmpURL)
	// uploader.StartUpload(fileName, rtmpURL, -1, 30*time.Second)

	exitc := make(chan os.Signal, 1)
	signal.Notify(exitc, syscall.SIGINT, syscall.SIGTERM, os.Interrupt, os.Kill)
	go func(fn, fa string) {
		<-exitc
		fmt.Println("Got Ctrl-C, cancelling")
		gcancel()
		cleanup(fn, fa)
		time.Sleep(2 * time.Second)
		// exit(0, fn, fa, nil)
	}(fileName, *fileArg)
	<-sr2.Done()
	glog.Infof("Streaming stread id=%s done, waiting 10 seconds", stream.ID)
	time.Sleep(10 * time.Second)
	// now get sessions
	sessions, err := lapi.GetSessions(stream.ID, false)
	if err != nil {
		glog.Errorf("Error getting sessions for stream id=%s err=%v", stream.ID, err)
		exit(252, fileName, *fileArg, err)
	}
	glog.Infof("Sessions: %+v", sessions)
	if len(sessions) != 1 {
		glog.Warningf("Should have one session, got %d", len(sessions))
		exit(251, fileName, *fileArg, err)
	}
	sess := sessions[0]
	if len(sess.Profiles) != len(stream.Profiles) {
		glog.Warningf("Got %d, but should have", len(sess.Profiles), len(stream.Profiles))
		exit(251, fileName, *fileArg, err)
	}
	if sess.RecordingStatus != livepeer.RecordingStatusWaiting {
		glog.Warningf("Recording status is %s but should be %s", sess.RecordingStatus, livepeer.RecordingStatusWaiting)
		exit(250, fileName, *fileArg, err)
	}

	glog.Info("Streaming done, waiting for recording URL to appear")
	if useForceURL {
		time.Sleep(5 * time.Second)
	} else {
		time.Sleep(6*time.Minute + 20*time.Second)
	}

	sessions, err = lapi.GetSessions(stream.ID, useForceURL)
	if err != nil {
		glog.Errorf("Error getting sessions for stream id=%s err=%v", stream.ID, err)
		exit(252, fileName, *fileArg, err)
	}
	glog.Infof("Sessions: %+v", sessions)

	sess = sessions[0]
	if sess.RecordingStatus != livepeer.RecordingStatusReady {
		glog.Warningf("Recording status is %s but should be %s", sess.RecordingStatus, livepeer.RecordingStatusReady)
		exit(250, fileName, *fileArg, err)
	}
	if sess.RecordingURL == "" {
		glog.Warningf("Recording URL should appear by now")
		exit(249, fileName, *fileArg, err)
	}
	glog.Infof("recordingURL=%s downloading now", sess.RecordingURL)

	// started := time.Now()
	// downloader := testers.NewM3utester2(gctx, sess.RecordingURL, false, false, false, false, 5*time.Second, nil)
	// <-downloader.Done()
	// glog.Infof(`Pulling stopped after %s`, time.Since(started))
	// exit(55, fileName, *fileArg, err)
	glog.Info("Done")
	// lapi.DeleteStream(stream.ID)
	// exit(0, fileName, *fileArg, err)
	es := checkDown(gctx, sess.RecordingURL, *testDuration)
	if es == 0 {
		lapi.DeleteStream(stream.ID)
		// exit(0, fileName, *fileArg, err)
	}
	exit(es, fileName, *fileArg, err)
}

func checkDown(gctx context.Context, url string, streamDuration time.Duration) int {
	es := 0
	started := time.Now()
	downloader := testers.NewM3utester2(gctx, url, false, false, false, false, 5*time.Second, nil)
	<-downloader.Done()
	glog.Infof(`Pulling stopped after %s`, time.Since(started))
	vs := downloader.VODStats()
	if len(vs.SegmentsNum) != len(standardProfiles)+1 {
		glog.Warningf("Number of renditions doesn't match! Has %d should %d", len(vs.SegmentsNum), len(standardProfiles)+1)
		es = 35
	}
	glog.Infof("Stats: %s", vs.String())
	glog.Infof("Stats raw: %+v", vs)
	if !vs.IsOk(streamDuration) {
		glog.Warningf("NOT OK!")
		es = 36
	} else {
		glog.Infoln("All ok!")
	}
	return es
}
