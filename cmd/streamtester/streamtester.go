// Stream tester is a tool to measure performance and stability of
// Livepeer transcoding network
package main

import (
	"flag"
	"fmt"
	"runtime"
	"time"

	"../../internal/testers"
	"github.com/golang/glog"
	"github.com/nareix/joy4/format"
)

func init() {
	format.RegisterAll()
}

// func startDownload(manifestID string) model.M3UTester {
// 	urlToRead := fmt.Sprintf("http://localhost:8935/stream/%s.m3u8", manifestID)
// 	tester := testers.NewM3UTester()
// 	tester.Start(urlToRead)
// 	return tester
// }

func main() {
	flag.Set("logtostderr", "true")
	version := flag.Bool("version", false, "Print out the version")
	number := flag.Uint("number", 1, "Number of simulteneous streams to stream")
	flag.Parse()

	if *version {
		fmt.Println("Stream tester version: 0.1")
		fmt.Printf("Compiler version: %s %s\n", runtime.Compiler, runtime.Version())
		return
	}
	fmt.Printf("Args: %+v\n", flag.Args())
	host := "localhost"
	rtmp := "1935"
	media := "8935"
	fn := "BigBuckBunny.mp4"
	// manfistID := "app"
	if len(flag.Args()) > 0 {
		fn = flag.Arg(0)
		// fnp := strings.Split(fn, ".")
		// manfistID = fnp[0]
		// if len(os.Args) > 2 {
		// 	manfistID = os.Args[2]
		// }
	}
	glog.Infof("Starting stream tester, file %s number of streams is %d", fn, *number)
	defer glog.Infof("Exiting")
	// exc := make(chan interface{})
	// go startUpload(exc, fn, manfistID)
	// tester := startDownload(manfistID)
	// <-exc
	sr := testers.NewStreamer()
	err := sr.StartStreams(fn, host, rtmp, media, *number)
	if err != nil {
		glog.Fatal(err)
	}
	go func() {
		for {
			time.Sleep(5 * time.Second)
			// fmt.Println(sr.StatsFormatted())
			fmt.Println(sr.Stats().FormatForConsole())
		}
	}()
	<-sr.Done()
	fmt.Println("========= Stats: =========")
	// fmt.Println(sr.StatsFormatted())
	fmt.Println(sr.Stats().FormatForConsole())
}
