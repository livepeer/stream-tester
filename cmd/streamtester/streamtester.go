// Stream tester is a tool to measure performance and stability of
// Livepeer transcoding network
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/peterbourgon/ff"

	"github.com/golang/glog"
	"github.com/livepeer/joy4/format"
	"github.com/livepeer/stream-tester/apis/livepeer"
	mistapi "github.com/livepeer/stream-tester/apis/mist"
	"github.com/livepeer/stream-tester/internal/metrics"
	"github.com/livepeer/stream-tester/internal/server"
	"github.com/livepeer/stream-tester/internal/testers"
	"github.com/livepeer/stream-tester/internal/utils"
	"github.com/livepeer/stream-tester/messenger"
	"github.com/livepeer/stream-tester/model"
)

func init() {
	format.RegisterAll()
}

func main() {
	flag.Set("logtostderr", "true")
	version := flag.Bool("version", false, "Print out the version")
	sim := flag.Uint("sim", 1, "Number of simulteneous streams to stream")
	repeat := flag.Uint("repeat", 1, "Number of time to repeat")
	profiles := flag.Int("profiles", 2, "Number of transcoding profiles configured on broadcaster")
	bhost := flag.String("host", "localhost", "Host name (usually broadcaster's) to stream RTMP stream to")
	mhost := flag.String("media-host", "", "Host name to read transcoded segments back from. -host will be used by default if not specified")
	rtmp := flag.String("rtmp", "1935", "RTMP port number")
	media := flag.String("media", "8935", "Media port number")
	stime := flag.String("time", "", "Time to stream streams (40s, 4m, 24h45m). Not compatible with repeat option.")
	fServer := flag.Bool("server", false, "Server mode")
	latency := flag.Bool("latency", false, "Measure latency")
	wowza := flag.Bool("wowza", false, "Wowza mode")
	mist := flag.Bool("mist", false, "Mist mode")
	noBar := flag.Bool("no-bar", false, "Do not show progress bar")
	serverAddr := flag.String("serverAddr", "localhost:7934", "Server address to bind to")
	infinitePull := flag.String("infinite-pull", "", "URL of .m3u8 to pull from")
	discordURL := flag.String("discord-url", "", "URL of Discord's webhook to send messages to Discord channel")
	discordUserName := flag.String("discord-user-name", "", "User name to use when sending messages to Discord")
	discordUsersToNotify := flag.String("discord-users", "", "Id's of users to notify in case of failure")
	latencyThreshold := flag.Float64("latency-threshold", 0, "Report failure to Discord if latency is bigger than specified")
	waitForTarget := flag.Duration("wait-for-target", 0, "How long to wait for RTMP target to appear")
	rtmpURL := flag.String("rtmp-url", "", "If RTMP URL specified, then infinite streamer will be used (for Wowza testing)")
	mediaURL := flag.String("media-url", "", "If RTMP URL specified, then infinite streamer will be used (for Wowza testing)")
	noExit := flag.Bool("no-exit", false, "Do not exit after test. For use in k8s as one-off job")
	save := flag.Bool("save", false, "Save downloaded segments")
	gsBucket := flag.String("gsbucket", "", "Google storage bucket (to store segments that was not successfully parsed)")
	gsKey := flag.String("gskey", "", "Google Storage private key (in json format)")
	azureStorageAccount := flag.String("azure-storage-account", "", "Azure storage account")
	azureAccessKey := flag.String("azure-access-key", "", "Azure access key")
	azureContainer := flag.String("azure-container", "", "Azure container")
	ignoreNoCodecError := flag.Bool("ignore-no-codec-error", false, "Do not stop streaming if segment without codec's info downloaded")
	ignoreGaps := flag.Bool("ignore-gaps", false, "Do not stop streaming if gaps found")
	ignoreTimeDrift := flag.Bool("ignore-time-drift", false, "Do not stop streaming if time drift detected")
	httpIngest := flag.Bool("http-ingest", false, "Use Livepeer HTTP HLS ingest")
	fileArg := flag.String("file", "", "File to stream")
	failHard := flag.Bool("fail-hard", false, "Panic if can't parse downloaded segments")
	mistCreds := flag.String("mist-creds", "", "login:password of the Mist server")
	mistPort := flag.Uint("mist-port", 4242, "Port of the Mist server")
	apiToken := flag.String("api-token", "", "Token of the Livepeer API to be used by the Mist server")
	apiServer := flag.String("api-server", "livepeer.com", "Server of the Livepeer API to be used")
	lapiFlag := flag.Bool("lapi", false, "Use Livepeer API to create streams. api-token should be specified")
	presets := flag.String("presets", "", "Comma separate list of transcoding profiels to use along with Livepeer API")
	skipTime := flag.Duration("skip-time", 0, "Skips first x(s|m)")
	picartoFlag := flag.Bool("picarto", false, "Do Picarto-pull testing")
	adult := flag.Bool("adult", false, "Adult Picarto")
	gaming := flag.Bool("gaming", false, "Gaming Picarto")
	picartoStreams := flag.Uint("picarto-streams", 1, "Number of streams to pull from Picarto")
	picartoBlackList := flag.String("picarto-black-list", "", "Picarto streams to ignore")
	picartoExternalHost := flag.String("picarto-external-host", "", "Host name of the Picarto server to be used in the messages to Discord")
	picartoStatsInterval := flag.Duration("picarto-stats-interval", 0, "Interval between stats messages sent to Discord")
	picartoSDCutOff := flag.Float64("picarto-standad-deviation-cutoff", 0.0, "Do not start streams that have standard deviation of segments durations more than that")
	delayStart := flag.Duration("delay-start", 0, "Delay start")
	botToken := flag.String("bot-token", "", "Discord's bot token")
	channelID := flag.String("channel-id", "", "Discord's channel id (can be list of channels, separated by comma)")
	statsFile := flag.String("stats-file", "", "Path to where to store the stream stats, in JSON")
	rtmpInfinitePush := flag.Bool("rtmp-infinite-push", false, "Just push file infinitely to -rtmp-url and do not read anything back")
	_ = flag.String("config", "", "config file (optional)")

	ff.Parse(flag.CommandLine, os.Args[1:],
		ff.WithConfigFileFlag("config"),
		ff.WithConfigFileParser(ff.PlainParser),
		ff.WithEnvVarPrefix("STREAM_TESTER"),
	)
	flag.Parse()

	hostName, _ := os.Hostname()
	fmt.Println("Stream tester version: " + model.Version)
	fmt.Printf("Compiler version: %s %s\n", runtime.Compiler, runtime.Version())
	fmt.Printf("Hostname %s OS %s IPs %v\n", hostName, runtime.GOOS, utils.GetIPs())
	fmt.Printf("Production: %v\n", model.Production)

	if *version {
		// fmt.Println("Stream tester version: " + model.Version)
		// fmt.Printf("Compiler version: %s %s\n", runtime.Compiler, runtime.Version())
		return
	}
	if *latencyThreshold > 0 {
		*latency = true
	}
	metrics.InitCensus(hostName, model.Version)
	// codec.MTest()
	// return
	gctx, gcancel := context.WithCancel(context.Background()) // to be used as global parent context, in the future
	messenger.Init(gctx, *discordURL, *discordUserName, *discordUsersToNotify, *botToken, *channelID, *apiToken)
	startWebServer := func() {
		exitc := make(chan os.Signal, 1)
		signal.Notify(exitc, syscall.SIGINT, syscall.SIGTERM, os.Interrupt, os.Kill)
		go func() {
			<-exitc
			fmt.Println("Got Ctrl-C, cancelling")
			gcancel()
		}()
		s := server.NewStreamerServer(*wowza, *apiToken, *mistCreds, *mistPort)
		s.StartWebServer(gctx, *serverAddr)
	}

	testers.Bucket = *gsBucket
	testers.CredsJSON = *gsKey
	if err := testers.AzureInit(*azureStorageAccount, *azureAccessKey, *azureContainer); err != nil {
		panic(err)
	}
	if *delayStart > 0 {
		glog.Infof("Waiting %s", *delayStart)
		time.Sleep(*delayStart)
	}
	if *infinitePull != "" {
		model.ProfilesNum = 0
		puller := testers.NewInfinitePuller(gctx, *infinitePull, *save, *wowza, *mist)
		puller.Start()
		return
	}
	var lapi *livepeer.API
	if *fServer {
		startWebServer()
		time.Sleep(2 * time.Second)
		return
	}
	fn := "official_test_source_2s_keys_24pfs.mp4"
	if len(flag.Args()) > 0 {
		fn = flag.Arg(0)
	}
	if *fileArg != "" {
		fn = *fileArg
	}
	model.ProfilesNum = *profiles
	model.FailHardOnBadSegments = *failHard
	var err error
	testers.IgnoreNoCodecError = *ignoreNoCodecError
	testers.IgnoreGaps = *ignoreGaps
	testers.IgnoreTimeDrift = *ignoreTimeDrift

	if *rtmpInfinitePush {
		if *rtmpURL == "" {
			glog.Error("-rtmp-url should be specified")
			return
		}
		// check if we can make TCP connection to RTMP target
		if err := utils.WaitForTCP(*waitForTarget, *rtmpURL); err != nil {
			glog.Error(err)
			return
		}
		uploader := testers.NewRtmpStreamer(gctx, gcancel, *rtmpURL)
		uploader.StartUpload(fn, *rtmpURL, -1, *waitForTarget)
		return
	}

	if *picartoFlag {
		var mapi *mistapi.API
		mcreds := strings.Split(*mistCreds, ":")
		if len(mcreds) != 2 {
			glog.Fatal("Mist server's credentials should be in form 'login:password'")
		}

		mapi = mistapi.NewMist(*bhost, mcreds[0], mcreds[1], *apiToken, *mistPort)
		mapi.Login()

		mc := testers.NewMistController(*bhost, int(*picartoStreams), *profiles, *adult, *gaming, *save, mapi,
			*picartoBlackList, *picartoExternalHost, *picartoStatsInterval, *picartoSDCutOff)
		// emsg := fmt.Sprintf("Starting **%d** Picarto streams (ver %s)", *picartoStreams, model.Version)
		// messenger.SendMessage(emsg)
		go startWebServer() // needed for /metrics endpoint

		err = mc.Start()
		if err != nil {
			emsg := fmt.Sprintf("Fatal error starting Picarto testing: %v", err)
			messenger.SendFatalMessage(emsg)
			time.Sleep(time.Second)
			panic(emsg)
		}
		emsg := fmt.Sprintf("Picarto streaming ended")
		messenger.SendFatalMessage(emsg)
		gcancel()
		time.Sleep(time.Second)
		return
	}
	var streamDuration time.Duration
	if *stime != "" {
		if streamDuration, err = server.ParseStreamDurationArgument(*stime); err != nil {
			panic(err)
		}
		if *repeat > 1 {
			// glog.Fatal("Can't set both -time and -repeat.")
		}
	}

	if *mediaURL != "" && *rtmpURL == "" {
		msg := fmt.Sprintf(`Starting infinite pull from %s`, *mediaURL)
		messenger.SendMessage(msg)
		sr2 := testers.NewStreamer2(gctx, *wowza, *mist)
		sr2.StartPulling(*mediaURL)
		return
	}
	if *rtmpURL != "" {
		if *mediaURL == "" {
			glog.Fatal("Should also specifiy -media-url")
		}
		msg := fmt.Sprintf(`Starting infinite stream to %s`, *mediaURL)
		messenger.SendMessage(msg)
		sr2 := testers.NewStreamer2(gctx, *wowza, *mist)
		sr2.StartStreaming(fn, *rtmpURL, *mediaURL, *waitForTarget, streamDuration)
		if *wowza {
			// let Wowza remove session
			time.Sleep(3 * time.Minute)
		}
		os.Exit(model.ExitCode)
		// to not exit
		// s := server.NewStreamerServer(*wowza)
		// s.StartWebServer("localhost:7933")
		return
	}

	mHost := *mhost
	if mHost == "" {
		mHost = *bhost
	}

	if *mist && (*mistCreds == "" || *apiToken == "") {
		glog.Fatal("If Mist server should be load-tested, then -mist-creds and -api-token should be specified. It is needed to create streams on Mist server using API.")
	}
	var mapi *mistapi.API
	if *mist {
		if *httpIngest {
			glog.Fatal("HTTP ingest can't be used for Mist server")
		}
		mcreds := strings.Split(*mistCreds, ":")
		if len(mcreds) != 2 {
			glog.Fatal("Mist server's credentials should be in form 'login:password'")
		}

		mapi = mistapi.NewMist(*bhost, mcreds[0], mcreds[1], *apiToken, *mistPort)
		mapi.Login()
		// mapi.CreateStream("dark1", "P720p30fps16x9")
		// mapi.DeleteStreams("dark1")
	}
	if *lapiFlag {
		if *apiToken == "" {
			glog.Fatalf("-api-token should be specified")
		}
		if !*httpIngest {
			// glog.Fatal("Using Livepeer API currently only implemented for HTTP ingest")
			// glog.Fatal("Using Livepeer API currently only implemented for RTMP ingest")
			// API webhook doesn't authenicate RTMP streams
		}
		if *presets == "" {
			// glog.Fatal("Presets should be specified")
		}
		go startWebServer() // needed for /metrics endpoint
		hostName, _ := os.Hostname()
		// presetsParts := strings.Split(*presets, ",")
		// model.ProfilesNum = len(presetsParts)
		lapi = livepeer.NewLivepeer(*apiToken, *apiServer, nil)
		lapi.Init()
		glog.Infof("Choosen server: %s", lapi.GetServer())
		ingests, err := lapi.Ingest(false)
		if err != nil {
			panic(err)
		}
		glog.Infof("Got ingests: %+v", ingests)
		streamName := fmt.Sprintf("%s_%s", hostName, time.Now().Format(time.RFC3339Nano))
		stream, err := lapi.CreateStreamEx(streamName)
		if err != nil {
			panic(err)
		}
		glog.Infof("Created stream %s", stream.ID)
		model.ProfilesNum = 4 // number of profiles that is created by CreateStreamEx
		if *httpIngest {
			bds, err := lapi.Broadcasters()
			if err != nil {
				panic(err)
			}
			glog.Infof("Got broadcasters to use: %v", bds)
			if len(bds) == 0 {
				glog.Fatal("Got empty list of broadcasterf from Livepeer API")
			}
			up := testers.NewHTTPStreamer(gctx, true, "baseManifestID")
			up.StartUpload(fn, bds[0]+"/live/"+stream.ID, stream.ID, 0, 0, streamDuration, 0)
			stats, err := up.Stats()
			if err != nil {
				panic(err)
			}
			fmt.Println("========= Stats: =========")
			fmt.Println(stats.FormatForConsole())
			fmt.Println(stats.FormatErrorsForConsole())
			if stats.SuccessRate != 1.0 {
				model.ExitCode = 127
			}
		} else {
			*rtmpURL = ingests[0].Ingest + "/" + stream.StreamKey
			*mediaURL = ingests[0].Playback + "/" + stream.PlaybackID + "/index.m3u8"
			dur := "infinite"
			if streamDuration > 0 {
				dur = streamDuration.String()
			}
			msg := fmt.Sprintf(`Starting %s stream to %s`, dur, *rtmpURL)
			glog.Info(msg)

			sr2 := testers.NewStreamer2(gctx, *wowza, *mist)
			sr2.StartStreaming(fn, *rtmpURL, *mediaURL, *waitForTarget, streamDuration)
		}
		if model.ExitCode == 0 {
			err = lapi.DeleteStream(stream.ID)
			if err != nil {
				glog.Errorf("Error deleting stream %s: %v", stream.ID, err)
			}
		}
		os.Exit(model.ExitCode)
		return
	}
	// fmt.Printf("Args: %+v\n", flag.Args())
	glog.Infof("Starting stream tester %s, file %s number of streams is %d, repeat %d times no bar %v", model.Version, fn, *sim, *repeat, *noBar)

	defer glog.Infof("Exiting")
	var sr model.Streamer
	if !*httpIngest {
		sr = testers.NewStreamer(gctx, gcancel, *wowza, *mist, mapi, lapi)
	} else {
		sr = testers.NewHTTPLoadTester(gctx, gcancel, lapi, *skipTime)
	}
	_, err = sr.StartStreams(fn, *bhost, *rtmp, mHost, *media, *sim, *repeat, streamDuration, false, *latency, *noBar, 3, 5*time.Second, *waitForTarget)
	if err != nil {
		glog.Fatal(err)
	}
	if *noBar {
		go func() {
			for {
				time.Sleep(25 * time.Second)
				stats, _ := sr.Stats("")
				fmt.Println(stats.FormatForConsole())
				// fmt.Println(sr.DownStatsFormatted())
			}
		}()
	}
	// go func() {
	// 	time.Sleep(10 * time.Second)
	// 	sr.Cancel()
	// }()
	// Catch interrupt signal to shut down transcoder
	exitc := make(chan os.Signal, 1)
	signal.Notify(exitc, syscall.SIGINT, syscall.SIGTERM, os.Interrupt, os.Kill)
	go func() {
		<-exitc
		fmt.Println("Got Ctrl-C, cancelling")
		gcancel()
		sr.Cancel()
		time.Sleep(2 * time.Second)
	}()
	glog.Infof("Waiting for test to complete")
	<-sr.Done()
	time.Sleep(2 * time.Second)
	fmt.Println("========= Stats: =========")
	stats, _ := sr.Stats("")
	fmt.Println(stats.FormatForConsole())
	fmt.Println(stats.FormatErrorsForConsole())

	if len(*statsFile) > 0 {
		jsonStats, err := json.Marshal(stats)
		if err != nil {
			fmt.Printf("Error: %s", err)
			glog.Errorf("Error marshalling stats err=%v", err)
			return
		}
		err = ioutil.WriteFile(*statsFile, jsonStats, 0644)
		if err != nil {
			fmt.Printf("Error: %s", err)
			glog.Errorf("Error saving stats file err=%v", err)
			return
		}
	}

	// fmt.Println(sr.AnalyzeFormatted(false))
	if *latencyThreshold > 0 && stats.TranscodedLatencies.P95 > 0 {
		// check latencies, report failure or success
		var msg string
		if float64(stats.TranscodedLatencies.P95)/float64(time.Second) > *latencyThreshold {
			// report failure
			msg = fmt.Sprintf(`Test failed: transcode P95 latency is %s which is bigger than threshold %v`, stats.TranscodedLatencies.P95, *latencyThreshold)
			messenger.SendFatalMessage(msg)
		} else {
			msg = fmt.Sprintf(`Test succeded: transcode P95 latency is %s which is lower than threshold %v`, stats.TranscodedLatencies.P95, *latencyThreshold)
			messenger.SendMessage(msg)
		}
		fmt.Println(msg)
	}
	if *noExit {
		s := server.NewStreamerServer(*wowza, "", "", *mistPort)
		s.StartWebServer(gctx, *serverAddr)
	}
	// messenger.SendMessage(sr.AnalyzeFormatted(true))
}
