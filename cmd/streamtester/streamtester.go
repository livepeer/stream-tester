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

	"github.com/golang/glog"
	"github.com/livepeer/stream-tester/internal/model"
	"github.com/livepeer/stream-tester/internal/server"
	"github.com/livepeer/stream-tester/internal/testers"
	"github.com/nareix/joy4/format"
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
	fServer := flag.Bool("server", false, "Server mode")
	serverAddr := flag.String("serverAddr", "localhost:7934", "Server address to bind to")
	flag.Parse()

	if *version {
		fmt.Println("Stream tester version: 0.1")
		fmt.Printf("Compiler version: %s %s\n", runtime.Compiler, runtime.Version())
		return
	}
	if *fServer {
		s := server.NewStreamerServer()
		s.StartWebServer(*serverAddr)
		return
	}
	// fmt.Printf("Args: %+v\n", flag.Args())
	fn := "BigBuckBunny.mp4"
	if len(flag.Args()) > 0 {
		fn = flag.Arg(0)
	}
	glog.Infof("Starting stream tester, file %s number of streams is %d, repeat %d times", fn, *sim, *repeat)

	defer glog.Infof("Exiting")
	model.ProfilesNum = *profiles
	sr := testers.NewStreamer()
	err := sr.StartStreams(fn, *host, *rtmp, *media, *sim, *repeat, false)
	if err != nil {
		glog.Fatal(err)
	}
	go func() {
		for {
			time.Sleep(25 * time.Second)
			fmt.Println(sr.Stats().FormatForConsole())
		}
	}()
	// Catch interrupt signal to shut down transcoder
	exitc := make(chan os.Signal)
	signal.Notify(exitc, os.Interrupt)
	go func() {
		<-exitc
		sr.Cancel()
	}()
	<-sr.Done()
	fmt.Println("========= Stats: =========")
	fmt.Println(sr.Stats().FormatForConsole())
}
