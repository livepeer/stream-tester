// Stream tester is a tool to measure performance and stability of
// Livepeer transcoding network
package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"time"

	"github.com/peterbourgon/ff"

	"github.com/golang/glog"
	"github.com/livepeer/joy4/format"
	"github.com/livepeer/stream-tester/internal/messenger"
	"github.com/livepeer/stream-tester/internal/model"
	"github.com/livepeer/stream-tester/internal/server"
	"github.com/livepeer/stream-tester/internal/testers"
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
	host := flag.String("host", "localhost", "Broadcaster's host name")
	rtmp := flag.String("rtmp", "1935", "RTMP port number")
	media := flag.String("media", "8935", "Media port number")
	stime := flag.String("time", "", "Time to stream streams (40s, 4m, 24h45m). Not compatible with repeat option.")
	fServer := flag.Bool("server", false, "Server mode")
	latency := flag.Bool("latency", false, "Measure latency")
	wowza := flag.Bool("wowza", false, "Wowza mode")
	noBar := flag.Bool("no-bar", false, "Do not show progress bar")
	serverAddr := flag.String("serverAddr", "localhost:7934", "Server address to bind to")
	infinitePull := flag.String("infinite-pull", "", "URL of .m3u8 to pull from")
	discordURL := flag.String("discord-url", "", "URL of Discord's webhook to send messages to Discord channel")
	discordUserName := flag.String("discord-user-name", "", "User name to use when sending messages to Discord")
	discordUsersToNotify := flag.String("discord-users", "", "Id's of users to notify in case of failure")
	latencyThreshold := flag.Float64("latency-threshold", 0, "Report failure to Discord if latency is bigger than specified")
	waitForTarget := flag.String("wait-for-target", "", "How long to wait for RTMP target to appear")
	rtmpURL := flag.String("rtmp-url", "", "If RTMP URL specified, then infinite streamer will be used (for Wowza testing)")
	mediaURL := flag.String("media-url", "", "If RTMP URL specified, then infinite streamer will be used (for Wowza testing)")
	noExit := flag.Bool("no-exit", false, "Do not exit after test. For use in k8s as one-off job")
	save := flag.Bool("save", false, "Save downloaded segments")
	gsBucket := flag.String("gsbucket", "", "Google storage bucket (to store segments that was not successfully parsed)")
	gsKey := flag.String("gskey", "", "Google Storage private key (in json format)")
	ignoreNoCodecError := flag.Bool("ignore-no-codec-error", false, "Do not stop streaming if segment without codec's info downloaded")
	ignoreGaps := flag.Bool("ignore-gaps", false, "Do not stop streaming if gaps found")
	ignoreTimeDrift := flag.Bool("ignore-time-drift", false, "Do not stop streaming if time drift detected")
	_ = flag.String("config", "", "config file (optional)")

	ff.Parse(flag.CommandLine, os.Args[1:],
		ff.WithConfigFileFlag("config"),
		ff.WithConfigFileParser(ff.PlainParser),
		ff.WithEnvVarPrefix("STREAM_TESTER"),
	)
	flag.Parse()

	if *version {
		fmt.Println("Stream tester version: 0.7")
		fmt.Printf("Compiler version: %s %s\n", runtime.Compiler, runtime.Version())
		return
	}
	if *latencyThreshold > 0 {
		*latency = true
	}
	messenger.Init(*discordURL, *discordUserName, *discordUsersToNotify)
	testers.Bucket = *gsBucket
	testers.CredsJSON = *gsKey
	if *infinitePull != "" {
		puller := testers.NewInfinitePuller(*infinitePull, *save)
		puller.Start()
		runtime.Goexit()
		return
	}
	if *fServer {
		s := server.NewStreamerServer(*wowza)
		s.StartWebServer(*serverAddr)
		return
	}
	fn := "official_test_source_2s_keys_24pfs.mp4"
	if len(flag.Args()) > 0 {
		fn = flag.Arg(0)
	}
	model.ProfilesNum = *profiles
	var err error
	var waitForDur time.Duration
	if *waitForTarget != "" {
		waitForDur, err = time.ParseDuration(*waitForTarget)
		if err != nil {
			panic(err)
		}
	}
	testers.IgnoreNoCodecError = *ignoreNoCodecError
	testers.IgnoreGaps = *ignoreGaps
	testers.IgnoreTimeDrift = *ignoreTimeDrift
	if *mediaURL != "" && *rtmpURL == "" {
		msg := fmt.Sprintf(`Starting infinite stream to %s`, *mediaURL)
		messenger.SendMessage(msg)
		sr2 := testers.NewStreamer2(*wowza)
		sr2.StartPulling(*mediaURL)
		return
	}
	if *rtmpURL != "" {
		if *mediaURL == "" {
			glog.Fatal("Should also specifiy -media-url")
		}
		msg := fmt.Sprintf(`Starting infinite stream to %s`, *mediaURL)
		messenger.SendMessage(msg)
		sr2 := testers.NewStreamer2(*wowza)
		sr2.StartStreaming(fn, *rtmpURL, *mediaURL, waitForDur)
		// let Wowza remove session
		time.Sleep(3 * time.Minute)
		// to not exit
		// s := server.NewStreamerServer(*wowza)
		// s.StartWebServer("localhost:7933")
		return
	}

	var streamDuration time.Duration
	if *stime != "" {
		if streamDuration, err = server.ParseStreamDurationArgument(*stime); err != nil {
			panic(err)
		}
		if *repeat > 1 {
			glog.Fatal("Can't set both -time and -repeat.")
		}
	}
	// fmt.Printf("Args: %+v\n", flag.Args())
	glog.Infof("Starting stream tester, file %s number of streams is %d, repeat %d times no bar %v", fn, *sim, *repeat, *noBar)

	defer glog.Infof("Exiting")
	sr := testers.NewStreamer(*wowza)
	// err = sr.StartStreams(fn, *host, *rtmp, *media, *sim, *repeat, streamDuration, *noBar, *latency, 3, 5*time.Second)
	err = sr.StartStreams(fn, *host, *rtmp, *media, *sim, *repeat, streamDuration, false, *latency, *noBar, 3, 5*time.Second, waitForDur)
	if err != nil {
		glog.Fatal(err)
	}
	/*
		go func() {
			for {
				time.Sleep(25 * time.Second)
				// fmt.Println(sr.Stats().FormatForConsole())
				// fmt.Println(sr.DownStatsFormatted())
			}
		}()
	*/
	// Catch interrupt signal to shut down transcoder
	exitc := make(chan os.Signal, 1)
	signal.Notify(exitc, os.Interrupt)
	go func() {
		<-exitc
		fmt.Println("Got Ctrl-C, cancelling")
		sr.Cancel()
	}()
	glog.Infof("Waiting for test to complete")
	<-sr.Done()
	time.Sleep(2 * time.Second)
	fmt.Println("========= Stats: =========")
	stats := sr.Stats()
	fmt.Println(stats.FormatForConsole())
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
		s := server.NewStreamerServer(*wowza)
		s.StartWebServer(*serverAddr)
	}
	// messenger.SendMessage(sr.AnalyzeFormatted(true))
}
