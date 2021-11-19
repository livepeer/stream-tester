// Health tester is a tool to test Livepeer API's Stream Health functionality
package main

import (
	"context"
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
	"github.com/livepeer/stream-tester/internal/app/recordtester"
	"github.com/livepeer/stream-tester/internal/metrics"
	"github.com/livepeer/stream-tester/internal/server"
	"github.com/livepeer/stream-tester/internal/testers"
	"github.com/livepeer/stream-tester/internal/utils"
	"github.com/livepeer/stream-tester/messenger"
	"github.com/livepeer/stream-tester/model"
	"github.com/peterbourgon/ff/v2"
)

const useForceURL = true

func init() {
	format.RegisterAll()
	rand.Seed(time.Now().UnixNano())
}

func main() {
	flag.Set("logtostderr", "true")
	vFlag := flag.Lookup("v")

	fs := flag.NewFlagSet("healthtester", flag.ExitOnError)

	verbosity := fs.String("v", "", "Log verbosity.  {4|5|6}")
	version := fs.Bool("version", false, "Print out the version")

	sim := fs.Int("sim", 1, "Load test using <sim> streams")
	testDuration := fs.Duration("test-dur", 0, "How long to run overall test")
	pauseDuration := fs.Duration("pause-dur", 0, "How long to wait between two consecutive RTMP streams that will comprise one user session")
	apiToken := fs.String("api-token", "", "Token of the Livepeer API to be used")
	apiServer := fs.String("api-server", "livepeer.com", "Server of the Livepeer API to be used")
	fileArg := fs.String("file", "bbb_sunflower_1080p_30fps_normal_t02.mp4", "File to stream")
	continuousTest := fs.Duration("continuous-test", 0, "Do continuous testing")
	useHttp := fs.Bool("http", false, "Do HTTP tests instead of RTMP")
	discordURL := fs.String("discord-url", "", "URL of Discord's webhook to send messages to Discord channel")
	discordUserName := fs.String("discord-user-name", "", "User name to use when sending messages to Discord")
	discordUsersToNotify := fs.String("discord-users", "", "Id's of users to notify in case of failure")
	pagerDutyIntegrationKey := fs.String("pagerduty-integration-key", "", "PagerDuty integration key")
	pagerDutyComponent := fs.String("pagerduty-component", "", "PagerDuty component")
	bind := fs.String("bind", "0.0.0.0:9090", "Address to bind metric server to")

	_ = fs.String("config", "", "config file (optional)")

	ff.Parse(fs, os.Args[1:],
		ff.WithConfigFileFlag("config"),
		ff.WithConfigFileParser(ff.PlainParser),
		ff.WithEnvVarPrefix("RT"),
	)
	flag.CommandLine.Parse(nil)
	vFlag.Value.Set(*verbosity)

	hostName, _ := os.Hostname()
	fmt.Println("Health Tester version: " + model.Version)
	fmt.Printf("Compiler version: %s %s\n", runtime.Compiler, runtime.Version())
	fmt.Printf("Hostname %s OS %s IPs %v\n", hostName, runtime.GOOS, utils.GetIPs())
	fmt.Printf("Production: %v\n", model.Production)

	if *version {
		return
	}
	if *fileArg == "" {
		fmt.Println("Must provide -file argument")
		os.Exit(1)
	}
	if *pauseDuration > 5*time.Minute {
		fmt.Println("Pause must be less than 5 min")
		os.Exit(1)
	}
	if *testDuration == 0 {
		glog.Fatalf("-test-dur must be specified")
	}
	if *apiToken == "" {
		glog.Fatalf("-api-token must be specified")
	}
	if *sim <= 0 {
		glog.Fatalf("-sim must be greater than 0")
	}

	metrics.InitCensus(hostName, model.Version, "healthtester")
	testers.IgnoreNoCodecError = true
	testers.IgnoreGaps = true
	testers.IgnoreTimeDrift = true
	testers.StartDelayBetweenGroups = 0
	model.ProfilesNum = 0

	var (
		err           error
		fileName      string
		gctx, gcancel = context.WithCancel(context.Background()) // to be used as global parent context, in the future
	)
	defer gcancel()

	if fileName, err = utils.GetFile(*fileArg, strings.ReplaceAll(hostName, ".", "_")); err != nil {
		if err == utils.ErrNotFound {
			fmt.Printf("File %s not found\n", *fileArg)
		} else {
			fmt.Printf("Error getting file %s: %v\n", *fileArg, err)
		}
		os.Exit(1)
	}

	var lapi *livepeer.API
	var createdAPIStreams []string
	cleanup := func(fn, fa string) {
		if fn != fa {
			os.Remove(fn)
		}
		// TODO: Uncomment this actual cleanup?
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

	lapi = livepeer.NewLivepeer2(*apiToken, *apiServer, nil, 8*time.Second)
	lapi.Init()
	glog.Infof("Choosen server: %s", lapi.GetServer())

	sigctx := contextUntilSignal(gctx, syscall.SIGINT, syscall.SIGTERM, os.Interrupt)
	go func(fn, fa string) {
		<-sigctx.Done()
		if gctx.Err() == nil {
			fmt.Println("Got Ctrl-C, cancelling")
			gcancel()
		}
		cleanup(fn, fa)
		time.Sleep(2 * time.Second)
	}(fileName, *fileArg)
	messenger.Init(gctx, *discordURL, *discordUserName, *discordUsersToNotify, "", "", "")

	if *continuousTest > 0 {
		metricServer := server.NewMetricsServer()
		go metricServer.Start(gctx, *bind)
		crt := recordtester.NewContinuousRecordTester(gctx, lapi, *pagerDutyIntegrationKey, *pagerDutyComponent, *useHttp, false)
		err := crt.Start(fileName, *testDuration, *pauseDuration, *continuousTest)
		if err != nil {
			glog.Warningf("Continuous test ended with err=%v", err)
		}
		exit(0, fileName, *fileArg, err)
		return
	}

	if *sim == 1 {
		// just one stream
		rt := recordtester.NewRecordTester(gctx, lapi, useForceURL, *useHttp, false)
		es, err := rt.Start(fileName, *testDuration, *pauseDuration)
		exit(es, fileName, *fileArg, err)
		return
	}
	type testResult struct {
		exitCode int
		err      error
	}
	var (
		results = make(chan testResult)
		wg      = sync.WaitGroup{}
		start   = time.Now()
	)
	for i := 0; i < *sim; i++ {
		if i > 0 {
			waitSec := 3 + rand.Intn(5)
			time.Sleep(time.Duration(waitSec) * time.Second)
		}
		wg.Add(1)
		go func(ii int) {
			defer wg.Done()
			rt := recordtester.NewRecordTester(gctx, lapi, useForceURL, *useHttp, false)
			les, lerr := rt.Start(fileName, *testDuration, *pauseDuration)
			glog.Infof("===> ii=%d les=%d lerr=%v", ii, les, lerr)
			results <- testResult{les, lerr}
		}(i)
	}
	go func() {
		wg.Wait()
		close(results)
	}()
	exitCode, successes := 0, 0
	for r := range results {
		if r.exitCode > exitCode {
			exitCode = r.exitCode
		}
		if r.exitCode == 0 && r.err == nil {
			successes++
		}
	}
	took := time.Since(start)
	glog.Infof("%d streams test ended in %s. success rate: %f%%", *sim, took, float64(successes)/float64(*sim)*100.0)
	time.Sleep(1 * time.Hour)
	exit(exitCode, fileName, *fileArg, err)
}

func contextUntilSignal(parent context.Context, sigs ...os.Signal) context.Context {
	ctx, cancel := context.WithCancel(parent)
	go func() {
		defer cancel()
		waitSignal(sigs...)
	}()
	return ctx
}

func waitSignal(sigs ...os.Signal) {
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, sigs...)
	defer signal.Stop(sigc)

	signal := <-sigc
	switch signal {
	case syscall.SIGINT:
		glog.Infof("Got Ctrl-C, shutting down")
	case syscall.SIGTERM:
		glog.Infof("Got SIGTERM, shutting down")
	default:
		glog.Infof("Got signal %d, shutting down", signal)
	}
}
