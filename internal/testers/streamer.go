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

	"github.com/livepeer/stream-tester/internal/model"
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

func (sr *streamer) Cancel() {
	close(sr.eof)
}

func (sr *streamer) StartStreams(sourceFileName, host, rtmpPort, mediaPort string, simStreams, repeat uint, notFinal bool) error {
	nRtmpPort, err := strconv.Atoi(rtmpPort)
	if err != nil {
		return err
	}
	nMediaPort, err := strconv.Atoi(mediaPort)
	if err != nil {
		return err
	}
	go func() {
		for i := 0; i < int(repeat); i++ {
			err := sr.startStreams(sourceFileName, host, nRtmpPort, nMediaPort, simStreams)
			if err != nil {
				glog.Fatal(err)
				return
			}
		}
		if !notFinal {
			close(sr.eof)
		}
	}()
	return nil
}

func (sr *streamer) startStreams(sourceFileName, host string, nRtmpPort, nMediaPort int, simStreams uint) error {
	fmt.Printf("Starting streaming %s to %s:%d, number of streams is %d\n", sourceFileName, host, nRtmpPort, simStreams)

	var wg sync.WaitGroup
	started := make(chan interface{})
	baseManfistID := strings.ReplaceAll(path.Base(sourceFileName), ".", "") + "_" + randName()
	go func() {
		for i := 0; i < int(simStreams); i++ {
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
			go findSkippedSegmentsNumber(up, down)
			sr.downloaders = append(sr.downloaders, down)
			down.Start(mediaURL)
			// put random delay before start of next stream
			time.Sleep(time.Duration(rand.Intn(2)+2) * time.Second)
		}
		started <- nil
	}()
	<-started
	wg.Wait()
	return nil
}

func findSkippedSegmentsNumber(rtmp *rtmpStreamer, mt *m3utester) {
	for {
		time.Sleep(3 * time.Second)
		if tm, ok := mt.GetFIrstSegmentTime(); ok {
			// check rtmp streamer
			// tm == 5
			// [2.5, 5,  7.5]
			for i, segTime := range rtmp.counter.segmentsStartTimes {
				if segTime >= tm {
					rtmp.skippedSegments = i + 1
					glog.V(model.VERBOSE).Infof("Found that %d segments was skipped, first segment time is %s, in rtmp %s",
						rtmp.skippedSegments, tm, segTime)
					return
				}
			}
			return
		}
	}
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
		Finished:     true,
	}
	for _, rs := range sr.uploaders {
		// Broadcaster always skips at lest first segment, and potentially more
		stats.SentSegments += rs.counter.segments - rs.skippedSegments
		if rs.connectionLost {
			stats.ConnectionLost++
		}
		if rs.active {
			stats.RTMPActiveStreams++
			stats.Finished = false
		}
	}
	for _, mt := range sr.downloaders {
		ds := mt.stats()
		stats.DownloadedSegments += ds.success
		stats.FailedToDownloadSegments += ds.fail
		stats.BytesDownloaded += ds.bytes
	}
	if stats.SentSegments > 0 {
		stats.SucessRate = float64(stats.DownloadedSegments) / ((float64(model.ProfilesNum) + 1) * float64(stats.SentSegments)) * 100
	}
	stats.ShouldHaveDownloadedSegments = (model.ProfilesNum + 1) * stats.SentSegments
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
