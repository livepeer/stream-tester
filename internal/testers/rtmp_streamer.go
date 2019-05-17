package testers

import (
	"time"

	"github.com/golang/glog"
	"github.com/nareix/joy4/av/avutil"
	"github.com/nareix/joy4/av/pktque"
	"github.com/nareix/joy4/format/rtmp"
)

// var segLen = 2 * time.Second
var segLen = 1400 * time.Millisecond

type rtmpStreamer struct {
	ingestURL string
	counter   *segmentsCounter
}

// source is local file name for now
func newRtmpStreamer(ingestURL, source string) *rtmpStreamer {
	return &rtmpStreamer{
		ingestURL: ingestURL,
		counter:   &segmentsCounter{segLen: segLen},
	}
}

func (rs *rtmpStreamer) startUpload(fn, manifestID string) {
	// file, err := avutil.Open("BigBuckBunny.mp4")
	file, err := avutil.Open(fn)
	// file, err := avutil.Open("output2.mp4")
	// file, err := avutil.Open("output2-20.mp4")
	// file, err := avutil.Open("output2-20-def.mp4")
	// file, err := avutil.Open("output-def.mp4")
	if err != nil {
		glog.Fatal(err)
	}
	conn, err := rtmp.Dial("rtmp://localhost:1935/" + manifestID)
	// conn, _ := avutil.Create("rtmp://localhost:1936/app/publish")
	if err != nil {
		glog.Fatal(err)
	}
	// filters := pktque.Filters{&pktque.Walltime{}, &printKeyFrame{}, &segmentsCounter{segLen: 2 * time.Second}}
	filters := pktque.Filters{&pktque.Walltime{}, &printKeyFrame{}, rs.counter}

	// demuxer := &pktque.FilterDemuxer{Demuxer: file, Filter: &pktque.Walltime{}}
	demuxer := &pktque.FilterDemuxer{Demuxer: file, Filter: filters}
	avutil.CopyFile(conn, demuxer)

	file.Close()
	// wait before closing connection, so we can recieve transcoded data
	time.Sleep(8 * time.Second)
	conn.Close()
}
