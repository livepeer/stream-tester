package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"

	"github.com/golang/glog"
	"github.com/peterbourgon/ff"

	"github.com/livepeer/joy4/format"
	"github.com/livepeer/joy4/format/rtmp"
)

func init() {
	format.RegisterAll()
}

func main() {
	flag.Set("logtostderr", "true")
	vFlag := flag.Lookup("v")

	fs := flag.NewFlagSet("rtmp-server", flag.ExitOnError)

	verbosity := fs.String("v", "", "Log verbosity.  {4|5|6}")
	version := fs.Bool("version", false, "Print out the version")
	_ = fs.String("config", "", "config file (optional)")

	ff.Parse(fs, os.Args[1:],
		ff.WithConfigFileFlag("config"),
		ff.WithConfigFileParser(ff.PlainParser),
		ff.WithEnvVarPrefix("RTMP_SERVER"),
	)
	flag.CommandLine.Parse(nil)
	vFlag.Value.Set(*verbosity)

	if *version {
		fmt.Println("RTMP debug server version: 0.1")
		fmt.Printf("Compiler version: %s %s\n", runtime.Compiler, runtime.Version())
		return
	}
	rtmp.Debug = true
	server := &rtmp.Server{
		Addr: "",
	}
	server.HandlePublish = func(conn *rtmp.Conn) {
		fmt.Printf("got publish %s\n", conn.URL)
		streams, err := conn.Streams()
		if err != nil {
			panic(err)
		}
		fmt.Printf("Got %d streams\n", len(streams))
		var videoidx, audioidx int8
		for i, st := range streams {
			if st.Type().IsAudio() {
				audioidx = int8(i)
			}
			if st.Type().IsVideo() {
				videoidx = int8(i)
			}
		}
		fmt.Printf("Video stream index %d, audio stream index %d\n", videoidx, audioidx)
		for {
			pkt, err := conn.ReadPacket()
			if err != nil {
				fmt.Printf("ReadPacket got error %v\n", err)
				if err == io.EOF {
					break
				}
				panic(err)
			}
			if pkt.IsKeyFrame && pkt.Idx == videoidx || true {
				fmt.Printf("Packet Is Keyframe %v Is Audio %v Is Video %v dts=%s cts=%s\n",
					pkt.IsKeyFrame, pkt.Idx == audioidx, pkt.Idx == videoidx, pkt.Time, pkt.CompositionTime)
			}
		}
	}
	err := server.ListenAndServe()
	if err != nil {
		glog.Error(err)
	}
}
