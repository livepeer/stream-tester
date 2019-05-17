package testers

import (
	"fmt"
	"math/rand"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang/glog"

	"../model"
)

// streamer streams multiple RTMP streams into broadcaster node,
// reads back source and transcoded segments and count them
// and calculates success rate from these numbers
type streamer struct {
	uploaders   []*rtmpStreamer
	downloaders []*m3utester
	eof         chan struct{}
}

// NewStreamer returns new streamer
func NewStreamer() model.Streamer {
	return &streamer{eof: make(chan struct{})}
}

func (sr *streamer) Done() <-chan struct{} {
	return sr.eof
}

func (sr *streamer) StartStreams(sourceFileName, host, rtmpPort, mediaPort string, number uint) error {
	nRtmpPort, err := strconv.Atoi(rtmpPort)
	if err != nil {
		return err
	}
	nMediaPort, err := strconv.Atoi(mediaPort)
	if err != nil {
		return err
	}
	// exc := make(chan interface{}, len(number))
	var wg sync.WaitGroup
	baseManfistID := strings.ReplaceAll(path.Base(sourceFileName), ".", "") + "_" + randName()
	go func() {
		for i := 0; i < int(number); i++ {
			manifesID := fmt.Sprintf("%s_%d", baseManfistID, i)
			rtmpURL := fmt.Sprintf("rtmp://%s:%d/%s", host, nRtmpPort, manifesID)
			mediaURL := fmt.Sprintf("http://%s:%d/stream/%s.m3u8", host, nMediaPort, manifesID)
			glog.Infof("RTMP: %s", rtmpURL)
			glog.Infof("MEDIA: %s", mediaURL)
			up := newRtmpStreamer(rtmpURL, sourceFileName)
			wg.Add(1)
			go func() {
				up.startUpload(sourceFileName, manifesID)
				wg.Done()
			}()
			sr.uploaders = append(sr.uploaders, up)
			down := newM3UTester()
			sr.downloaders = append(sr.downloaders, down)
			down.Start(mediaURL)
			// put random delay before start of next stream
			time.Sleep(time.Duration(rand.Intn(2)+2) * time.Second)
		}
		wg.Wait()
		close(sr.eof)
	}()
	return nil
}

func (sr *streamer) StatsFormatted() string {
	r := ""
	for _, md := range sr.downloaders {
		r += md.StatsFormatted()
	}
	return r
}

func (sr *streamer) Stats() *model.Stats {
	stats := &model.Stats{
		RTMPstreams:  len(sr.uploaders),
		MediaStreams: len(sr.downloaders),
	}
	for _, rs := range sr.uploaders {
		// Broadcaster always skips first segment
		stats.SentSegments += rs.counter.segments - 1
	}
	for _, mt := range sr.downloaders {
		ds := mt.stats()
		stats.DownloadedSegments += ds.success
		stats.FailedToDownloadSegments += ds.fail
		stats.BytesDownloaded += ds.bytes
	}
	return stats
}

func randName() string {
	rand.Seed(time.Now().UnixNano())
	x := make([]byte, 10, 10)
	for i := 0; i < len(x); i++ {
		x[i] = byte(rand.Uint32())
	}
	return fmt.Sprintf("%x", x)
}
