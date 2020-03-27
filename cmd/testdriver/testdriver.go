package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/livepeer/stream-tester/internal/testdriver"
	"github.com/livepeer/stream-tester/internal/utils"
	"github.com/livepeer/stream-tester/messenger"
	"github.com/livepeer/stream-tester/model"
	"github.com/peterbourgon/ff/v2"
)

func main() {
	flag.Set("logtostderr", "true")
	vFlag := flag.Lookup("v")

	fs := flag.NewFlagSet("testdriver", flag.ExitOnError)

	verbosity := fs.String("v", "", "Log verbosity.  {4|5|6}")
	streamTesterHost := fs.String("tester-host", "streamtester", "stream-tester hostname")
	streamTesterPort := fs.Uint("tester-port", 7934, "stream-tester http port")
	ingestHost := fs.String("ingest-host", "localost", "hostname of the tested broadcaster")
	rtmpPort := fs.Uint("rtmp-port", 1935, "rtmp port")
	mediaPort := fs.Uint("media-port", 8935, "media port")
	httpIngest := fs.Bool("http-ingest", false, "use HTTP ingest")
	fileArg := fs.String("file", "", "File to stream")
	stime := fs.String("time", "", "Time to stream streams (40s, 4m, 24h45m). Not compatible with repeat option.")
	presets := fs.String("presets", "", "Comma separate list of transcoding profiels to use along with Livepeer API")
	mist := fs.Bool("mist", false, "use Mist mode")

	startDelay := fs.Duration("start-delay", 15*time.Second, "time delay before start")

	statsInterval := fs.Duration("stats-interval", 10*time.Second, "time interval between checking stats")

	minSuccessRate := fs.Float64("min-success-rate", 99.0, "minimum success rate required to continue testing an increased number of streams")
	numProfiles := fs.Uint("profiles", 2, "number of profiles to test")
	numStreamsInit := fs.Uint("streams-init", 1, "number of streams to begin tests with")
	numStreamsStep := fs.Uint("streams-step", 1, "number of streams to increase by on each successive test")

	discordURL := fs.String("discord-url", "", "URL of Discord's webhook to send messages to Discord channel")
	discordUserName := fs.String("discord-user-name", "", "User name to use when sending messages to Discord")
	discordUsersToNotify := fs.String("discord-users", "", "User IDs to notify in case of failure")

	_ = fs.String("config", "", "config file (optional)")

	// httpPort := flag.Int("http-port", 80, "testdriver http port")

	// flag.Parse()
	ff.Parse(fs, os.Args[1:],
		ff.WithConfigFileFlag("config"),
		ff.WithConfigFileParser(ff.PlainParser),
		ff.WithEnvVarPrefix("TESTDRIVER"),
	)
	flag.CommandLine.Parse(nil)
	vFlag.Value.Set(*verbosity)

	hostName, _ := os.Hostname()
	fmt.Println("testdriver version: " + model.Version)
	fmt.Printf("Compiler version: %s %s\n", runtime.Compiler, runtime.Version())
	fmt.Printf("Hostname %s OS %s IPs %v\n", hostName, runtime.GOOS, utils.GetIPs())

	messenger.Init(context.Background(), *discordURL, *discordUserName, *discordUsersToNotify, "", "", "")

	httpClient := &http.Client{
		Timeout: 8 * time.Second,
	}
	if *presets != "" {
		pp := strings.Split(*presets, ",")
		*numProfiles = uint(len(pp))
	}
	sc := &model.StartStreamsReq{
		FileName:       *fileArg,
		HTTPIngest:     *httpIngest,
		RTMP:           uint16(*rtmpPort),
		Media:          uint16(*mediaPort),
		Host:           *ingestHost,
		Repeat:         1,
		ProfilesNum:    int(*numProfiles),
		MeasureLatency: true,
		Time:           *stime,
		Lapi:           true,
		Mist:           *mist,
		Presets:        *presets,
		// Presets:        "P240p30fps16x9,P360p30fps16x9,P576p30fps16x9,P720p30fps16x9",
	}
	testDriver := testdriver.NewTester(
		httpClient,
		*streamTesterHost,
		*streamTesterPort,
		*statsInterval,
		*minSuccessRate,
		*numStreamsInit,
		*numStreamsStep,
		sc,
		func(s string, r *testdriver.Result) {
			messenger.SendMessage(fmt.Sprintf("%s rtmp host: %s:%d / media port: %d using streamtester host: %s:%d - now testing **%d** concurrent streams with %d profiles",
				s, *ingestHost, *rtmpPort, *mediaPort, *streamTesterHost, *streamTesterPort, r.NumStreams, r.NumProfiles))
		},
	)

	resultString := func(res *testdriver.Result) string {
		return fmt.Sprintf(
			"concurrency test results for rtmp host: %s:%d / media port: %d using streamtester host: %s:%d - success rate degrades at %d streams:\n ```%s```\n```%s```",
			*ingestHost, *rtmpPort, *mediaPort, *streamTesterHost, *streamTesterPort, res.NumStreams, res.Stats.FormatForConsole(), res.Stats.FormatErrorsForConsole())
	}

	// channel for catching benchmark results to be served on endpoint
	// ch := make(chan *testdriver.Result)

	/*
		if *httpPort > 0 {
			go func() {
				e := echo.New()

				e.Use(middleware.Logger())
				e.Use(middleware.Recover())

				var res *testdriver.Result

				go func() { res = <-ch }()

				// Route => handler
				e.GET("/", func(c echo.Context) error {
					if testDriver.IsRunning() {
						return c.String(http.StatusOK, "Benchmark is currently running.\n")
					}
					if testDriver.GetManifestID() == "" {
						return c.String(http.StatusOK, "Benchmark is not running.\n")
					}
					return c.String(http.StatusOK, resultString(res))
				})

				// Start server
				e.Logger.Fatal(e.Start(fmt.Sprintf(":%d", *httpPort)))
			}()
		}
	*/

	if model.Production {
		time.Sleep(*startDelay)
	}

	// FIXME: add random generated string to use as unique identifier
	messenger.SendMessage(
		fmt.Sprintf(
			"concurrency test started for rtmp host: %s:%d / media port %d using streamtester host: %s:%d",
			*ingestHost, *rtmpPort, *mediaPort, *streamTesterHost, *streamTesterPort))

	ctx := context.Background()

	res, err := testDriver.Run(ctx, *numProfiles)
	if err != nil {
		messenger.SendFatalMessage(fmt.Sprintf("FAILED: %v", err))
		log.Fatalf("FAILED: %v", err)
	}

	// send res into channel async
	// go func() { ch <- res }()

	messenger.SendMessage(resultString(res))
}
