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
	save := flag.Bool("save", false, "Save downloaded segments")
	_ = flag.String("config", "", "config file (optional)")

	ff.Parse(flag.CommandLine, os.Args[1:],
		ff.WithConfigFileFlag("config"),
		ff.WithConfigFileParser(ff.PlainParser),
		ff.WithEnvVarPrefix("STREAM_TESTER"),
	)
	flag.Parse()

	if *version {
		fmt.Println("Stream tester version: 0.2")
		fmt.Printf("Compiler version: %s %s\n", runtime.Compiler, runtime.Version())
		return
	}
	messenger.Init(*discordURL, *discordUserName)
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
	var streamDuration time.Duration
	var err error
	if *stime != "" {
		if streamDuration, err = server.ParseStreamDurationArgument(*stime); err != nil {
			panic(err)
		}
		if *repeat > 1 {
			glog.Fatal("Can't set both -time and -repeat.")
		}
	}
	// fmt.Printf("Args: %+v\n", flag.Args())
	fn := "official_test_source_2s_keys_24pfs.mp4"
	if len(flag.Args()) > 0 {
		fn = flag.Arg(0)
	}
	glog.Infof("Starting stream tester, file %s number of streams is %d, repeat %d times no bar %v", fn, *sim, *repeat, *noBar)

	defer glog.Infof("Exiting")
	model.ProfilesNum = *profiles
	sr := testers.NewStreamer(*wowza)
	// err = sr.StartStreams(fn, *host, *rtmp, *media, *sim, *repeat, streamDuration, *noBar, *latency, 3, 5*time.Second)
	err = sr.StartStreams(fn, *host, *rtmp, *media, *sim, *repeat, streamDuration, false, *latency, 3, 5*time.Second)
	if err != nil {
		glog.Fatal(err)
	}
	go func() {
		for {
			time.Sleep(25 * time.Second)
			// fmt.Println(sr.Stats().FormatForConsole())
			// fmt.Println(sr.DownStatsFormatted())
		}
	}()
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
	fmt.Println(sr.Stats().FormatForConsole())
	fmt.Println(sr.AnalyzeFormatted(false))
	// messenger.SendMessage(sr.AnalyzeFormatted(true))
}
