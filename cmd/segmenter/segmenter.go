package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path"
	"runtime"
	"time"

	"github.com/golang/glog"
	"github.com/peterbourgon/ff"

	"github.com/livepeer/joy4/av"
	"github.com/livepeer/joy4/av/avutil"
	"github.com/livepeer/joy4/format"
	"github.com/livepeer/joy4/jerrors"
)

var segLen = 2 * time.Second

func init() {
	format.RegisterAll()
}

func main() {
	flag.Set("logtostderr", "true")
	version := flag.Bool("version", false, "Print out the version")
	_ = flag.String("config", "", "config file (optional)")

	ff.Parse(flag.CommandLine, os.Args[1:],
		ff.WithConfigFileFlag("config"),
		ff.WithConfigFileParser(ff.PlainParser),
		ff.WithEnvVarPrefix("MEDIA_INFO"),
	)
	flag.Parse()

	if *version {
		fmt.Println("Segmenter version: 0.1")
		fmt.Printf("Compiler version: %s %s\n", runtime.Compiler, runtime.Version())
		return
	}
	if len(flag.Args()) == 0 {
		fmt.Println("Must specify file name.")
		// return
	}
	fileName := flag.Arg(0)
	// fileName = "/Users/dark/projects/livepeer/stream-tester/official_test_source_2s_keys_24pfs.mp4"
	fmt.Printf("Segmenting info about %s\n", fileName)
	file, err := avutil.Open(fileName)
	if err != nil {
		glog.Fatal(err)
	}
	dir := ""
	dir = flag.Arg(1)
	// dir = "/tmp/04"
	var streams []av.CodecData
	var videoidx, audioidx int8
	if streams, err = file.Streams(); err != nil {
		msg := fmt.Sprintf("Can't get info about file: '%+v', isNoAudio %v isNoVideo %v", err, errors.Is(err, jerrors.ErrNoAudioInfoFound), errors.Is(err, jerrors.ErrNoVideoInfoFound))
		if !(errors.Is(err, jerrors.ErrNoAudioInfoFound) || errors.Is(err, jerrors.ErrNoVideoInfoFound)) {
			glog.Fatal(msg)
		}
		fmt.Println(msg)
	}
	for i, st := range streams {
		if st.Type().IsAudio() {
			audioidx = int8(i)
		}
		if st.Type().IsVideo() {
			videoidx = int8(i)
		}
	}
	fmt.Printf("Video stream index %d, audio stream index %d\n", videoidx, audioidx)
	seqNo := 0
	// var curPTS time.Duration
	var firstFramePacket *av.Packet
	var prevPTS time.Duration
	for {
		segName := fmt.Sprintf("segment_%d.ts", seqNo)
		if dir != "" {
			segName = path.Join(dir, segName)
		}
		segFile, err := avutil.Create(segName)
		if err != nil {
			glog.Fatal(err)
		}
		err = segFile.WriteHeader(streams)
		if err != nil {
			glog.Fatal(err)
		}
		if firstFramePacket != nil {
			err = segFile.WritePacket(*firstFramePacket)
			if err != nil {
				glog.Fatal(err)
			}
			firstFramePacket = nil
		}
		// var curSegStart = curPTS
		var rerr error
		var pkt av.Packet
		for {
			pkt, rerr = file.ReadPacket()
			if rerr != nil {
				if rerr == io.EOF {
					break
				}
				glog.Fatal(rerr)
			}

			// fmt.Printf("Packet Is Keyframe %v Is Audio %v Is Video %v PTS %s\n", pkt.IsKeyFrame, pkt.Idx == audioidx, pkt.Idx == videoidx, pkt.Time)
			// curPTS = pkt.Time
			// if curPTS-curSegStart > 1900*time.Millisecond && pkt.IsKeyFrame {
			// 	firstFramePacket = &pkt
			// 	break
			// }
			// This matches segmenter algorithm used in ffmpeg
			if pkt.IsKeyFrame && pkt.Time >= time.Duration(seqNo+1)*segLen {
				firstFramePacket = &pkt
				fmt.Printf("Packet Is Keyframe %v Is Audio %v Is Video %v PTS %s sinc prev %s seqNo %d\n", pkt.IsKeyFrame, pkt.Idx == audioidx, pkt.Idx == videoidx, pkt.Time,
					pkt.Time-prevPTS, seqNo)
				prevPTS = pkt.Time
				break
			}
			err = segFile.WritePacket(pkt)
			if err != nil {
				glog.Fatal(err)
			}
		}
		err = segFile.WriteTrailer()
		if err != nil {
			glog.Fatal(err)
		}
		if rerr == io.EOF {
			break
		}
		seqNo++
	}
}
