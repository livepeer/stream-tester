package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"runtime"

	"github.com/golang/glog"
	"github.com/peterbourgon/ff"

	"github.com/livepeer/joy4/av"
	"github.com/livepeer/joy4/av/avutil"
	"github.com/livepeer/joy4/format"
	"github.com/livepeer/joy4/format/mp4"
	"github.com/livepeer/joy4/jerrors"
	"github.com/livepeer/stream-tester/internal/utils"
)

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
		fmt.Println("Media info version: 0.1")
		fmt.Printf("Compiler version: %s %s\n", runtime.Compiler, runtime.Version())
		return
	}
	if len(flag.Args()) == 0 {
		fmt.Println("Must specify file name.")
		return
	}
	fileName := flag.Arg(0)
	fmt.Printf("Gathering info about %s\n", fileName)

	var ext string
	var err error
	var u *url.URL
	// var file av.DemuxCloser
	var file av.Demuxer
	if u, _ = url.Parse(fileName); u != nil && u.Scheme != "" {
		ext = path.Ext(u.Path)
	} else {
		ext = path.Ext(fileName)
	}
	if u.Scheme != "" && ext == ".mp4" {
		sh := utils.NewSeekingHTTP(fileName)

		file = mp4.NewDemuxer(sh)
	} else {
		file, err = avutil.Open(fileName)
		if err != nil {
			glog.Fatal(err)
		}
	}
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
	for {
		pkt, err := file.ReadPacket()
		if err != nil {
			if err == io.EOF {
				break
			}
			glog.Fatal(err)
		}
		if pkt.IsKeyFrame && pkt.Idx == videoidx || false {
			fmt.Printf("Packet Is Keyframe %v Is Audio %v Is Video %v PTS %s CompTime %s\n",
				pkt.IsKeyFrame, pkt.Idx == audioidx, pkt.Idx == videoidx, pkt.Time, pkt.CompositionTime)
		}
	}
}
