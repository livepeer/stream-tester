// Record tester is a tool to test Livepeer API's recording functionality
package main

import (
	"context"
	"encoding/json"
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
	livepeerAPI "github.com/livepeer/go-api-client"
	"github.com/livepeer/joy4/format"
	"github.com/livepeer/livepeer-data/pkg/client"
	"github.com/livepeer/stream-tester/internal/app/recordtester"
	"github.com/livepeer/stream-tester/internal/metrics"
	"github.com/livepeer/stream-tester/internal/server"
	"github.com/livepeer/stream-tester/internal/testers"
	"github.com/livepeer/stream-tester/internal/utils"
	"github.com/livepeer/stream-tester/messenger"
	"github.com/livepeer/stream-tester/model"
	"github.com/peterbourgon/ff/v2"
)

func init() {
	format.RegisterAll()
	rand.Seed(time.Now().UnixNano())
}

func main() {
	flag.Set("logtostderr", "true")
	vFlag := flag.Lookup("v")

	fs := flag.NewFlagSet("recordtester", flag.ExitOnError)

	verbosity := fs.String("v", "", "Log verbosity.  {4|5|6}")
	version := fs.Bool("version", false, "Print out the version")

	sim := fs.Int("sim", 0, "Load test using <sim> streams")
	testDuration := fs.Duration("test-dur", 0, "How long to run overall test")
	pauseDuration := fs.Duration("pause-dur", 0, "How long to wait between two consecutive RTMP streams that will comprise one user session")
	apiToken := fs.String("api-token", "", "Token of the Livepeer API to be used")
	apiServer := fs.String("api-server", "livepeer.com", "Server of the Livepeer API to be used")
	ingestStr := fs.String("ingest", "", "Ingest server info in JSON format including ingest and playback URLs. Should follow Livepeer API schema")
	analyzerServers := fs.String("analyzer-servers", "", "Comma-separated list of base URLs to connect for the Stream Health Analyzer API (defaults to --api-server)")
	fileArg := fs.String("file", "bbb_sunflower_1080p_30fps_normal_t02.mp4", "File to stream")
	continuousTest := fs.Duration("continuous-test", 0, "Do continuous testing")
	useHttp := fs.Bool("http", false, "Do HTTP tests instead of RTMP")
	testMP4 := fs.Bool("mp4", false, "Download MP4 of recording")
	testStreamHealth := fs.Bool("stream-health", false, "Check stream health during test")
	recordObjectStoreId := fs.String("record-object-store-id", "", "ID for the Object Store to use for recording storage. Forwarded to the streams created in the API")
	discordURL := fs.String("discord-url", "", "URL of Discord's webhook to send messages to Discord channel")
	discordUserName := fs.String("discord-user-name", "", "User name to use when sending messages to Discord")
	discordUsersToNotify := fs.String("discord-users", "", "Id's of users to notify in case of failure")
	pagerDutyIntegrationKey := fs.String("pagerduty-integration-key", "", "PagerDuty integration key")
	pagerDutyComponent := fs.String("pagerduty-component", "", "PagerDuty component")
	pagerDutyLowUrgency := fs.Bool("pagerduty-low-urgency", false, "Whether to send only low-urgency PagerDuty alerts")
	bind := fs.String("bind", "0.0.0.0:9090", "Address to bind metric server to")

	_ = fs.String("config", "", "config file (optional)")

	ff.Parse(fs, os.Args[1:],
		ff.WithConfigFileFlag("config"),
		ff.WithConfigFileParser(ff.PlainParser),
		ff.WithEnvVarPrefix("RT"),
		ff.WithEnvVarIgnoreCommas(true),
	)
	flag.CommandLine.Parse(nil)
	vFlag.Value.Set(*verbosity)

	hostName, _ := os.Hostname()
	fmt.Println("Recordtester version: " + model.Version)
	fmt.Printf("Compiler version: %s %s\n", runtime.Compiler, runtime.Version())
	fmt.Printf("Hostname %s OS %s IPs %v\n", hostName, runtime.GOOS, utils.GetIPs())
	fmt.Printf("Production: %v\n", model.Production)

	if *version {
		return
	}
	metrics.InitCensus(hostName, model.Version, "recordtester")
	testers.IgnoreNoCodecError = true
	testers.IgnoreGaps = true
	testers.IgnoreTimeDrift = true
	testers.StartDelayBetweenGroups = 0
	model.ProfilesNum = 0

	if *fileArg == "" {
		fmt.Println("Should provide -file argument")
		os.Exit(1)
	}
	if *pauseDuration > 5*time.Minute {
		fmt.Println("Pause should be less than 5 min")
		os.Exit(1)
	}
	if *analyzerServers == "" {
		*analyzerServers = *apiServer
	}
	var fileName string
	var err error

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

	var ingest *livepeerAPI.Ingest
	if *ingestStr != "" {
		if err := json.Unmarshal([]byte(*ingestStr), &ingest); err != nil {
			glog.Fatalf("Error parsing -ingest argument: %v", err)
		}
	}

	var lapi *livepeerAPI.Client
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
		if err != context.Canceled {
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			}
			if exitCode != 0 {
				glog.Errorf("Record test failed exitCode=%d err=%v", exitCode, err)
			}
		} else {
			exitCode = 0
		}
		os.Exit(exitCode)
	}

	lApiOpts := livepeerAPI.ClientOptions{
		Server:      *apiServer,
		AccessToken: *apiToken,
		Timeout:     8 * time.Second,
	}
	lapi, _ = livepeerAPI.NewAPIClientGeolocated(lApiOpts)
	glog.Infof("Choosen server: %s", lapi.GetServer())

	userAgent := model.AppName + "/" + model.Version
	lanalyzers := testers.AnalyzerByRegion{}
	for _, url := range strings.Split(*analyzerServers, ",") {
		lanalyzers[url] = client.NewAnalyzer(url, *apiToken, userAgent, 0)
	}

	/*
		sessionsx, err := lapi.GetSessions("1f770f0a-9177-49bd-a848-023abee7c09b")
		if err != nil {
			glog.Errorf("Error getting sessions for stream id=%s err=%v", ".ID", err)
			exit(252, fileName, *fileArg, err)
		}
		glog.Infof("Sessions: %+v", sessionsx)
	*/

	exitc := make(chan os.Signal, 1)
	signal.Notify(exitc, syscall.SIGINT, syscall.SIGTERM, os.Interrupt)
	go func(fn, fa string) {
		<-exitc
		fmt.Println("Got Ctrl-C, cancelling")
		gcancel()
		cleanup(fn, fa)
		time.Sleep(2 * time.Second)
		// exit(0, fn, fa, nil)
	}(fileName, *fileArg)
	messenger.Init(gctx, *discordURL, *discordUserName, *discordUsersToNotify, "", "", "")

	rtOpts := recordtester.RecordTesterOptions{
		Client:              lapi,
		Analyzers:           lanalyzers,
		Ingest:              ingest,
		RecordObjectStoreId: *recordObjectStoreId,
		UseForceURL:         true,
		UseHTTP:             *useHttp,
		TestMP4:             *testMP4,
		TestStreamHealth:    *testStreamHealth,
	}
	if *sim > 1 {
		var testers []recordtester.IRecordTester
		var eses []int
		var wg sync.WaitGroup
		var es int
		var err error
		start := time.Now()

		for i := 0; i < *sim; i++ {
			rt := recordtester.NewRecordTester(gctx, rtOpts)
			eses = append(eses, 0)
			testers = append(testers, rt)
			wg.Add(1)
			go func(ii int) {
				les, lerr := rt.Start(fileName, *testDuration, *pauseDuration)
				glog.Infof("===> ii=%d les=%d lerr=%v", ii, les, lerr)
				eses[ii] = les
				if les != 0 {
					es = les
				}
				if lerr != nil {
					err = lerr
				}
				wg.Done()
			}(i)
			wait := time.Duration((3 + rand.Intn(5))) * time.Second
			time.Sleep(wait)
		}
		wg.Wait()
		var succ int
		for _, r := range eses {
			if r == 0 {
				succ++
			}
		}
		took := time.Since(start)
		glog.Infof("%d streams test ended in %s success %f%%", *sim, took, float64(succ)/float64(len(eses))*100.0)
		time.Sleep(1 * time.Hour)
		exit(es, fileName, *fileArg, err)
		return
	} else if *continuousTest > 0 {
		metricServer := server.NewMetricsServer()
		go metricServer.Start(gctx, *bind)
		crtOpts := recordtester.ContinuousRecordTesterOptions{
			PagerDutyIntegrationKey: *pagerDutyIntegrationKey,
			PagerDutyComponent:      *pagerDutyComponent,
			PagerDutyLowUrgency:     *pagerDutyLowUrgency,
			RecordTesterOptions:     rtOpts,
		}
		crt := recordtester.NewContinuousRecordTester(gctx, crtOpts)
		err := crt.Start(fileName, *testDuration, *pauseDuration, *continuousTest)
		if err != nil {
			glog.Warningf("Continuous test ended with err=%v", err)
		}
		exit(0, fileName, *fileArg, err)
		return
	}
	// just one stream
	rt := recordtester.NewRecordTester(gctx, rtOpts)
	es, err := rt.Start(fileName, *testDuration, *pauseDuration)
	exit(es, fileName, *fileArg, err)
}
