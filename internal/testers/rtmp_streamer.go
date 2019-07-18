package testers

import (
	"io"
	"time"

	"net"

	"github.com/golang/glog"
	"github.com/gosuri/uiprogress"
	"github.com/livepeer/joy4/av"
	"github.com/livepeer/joy4/av/avutil"
	"github.com/livepeer/joy4/av/pktque"
	"github.com/livepeer/joy4/format/rtmp"
	"github.com/livepeer/stream-tester/internal/utils"
)

var segLen = 2 * time.Second

// rtmpStreamer streams one video file to RTMP server
type rtmpStreamer struct {
	ingestURL       string
	counter         *segmentsCounter
	skippedSegments int
	connectionLost  bool
	active          bool
	done            chan struct{}
	file            av.DemuxCloser
}

// source is local file name for now
func newRtmpStreamer(ingestURL, source string, sentTimesMap *utils.SyncedTimesMap, bar *uiprogress.Bar, done chan struct{}) *rtmpStreamer {
	return &rtmpStreamer{
		done:            done,
		ingestURL:       ingestURL,
		counter:         newSegmentsCounter(segLen, bar, false, sentTimesMap),
		skippedSegments: 1, // Broadcaster always skips first segment, but can skip more - this will be corrected when first
		// segment downloaded back
	}
}

// GetNumberOfSegments returns number of segments in video file
func GetNumberOfSegments(fileName string, streamDuration time.Duration) int {
	file, err := avutil.Open(fileName)
	if err != nil {
		glog.Fatal(err)
	}
	recordSegmentsDurations := streamDuration > 0
	sc := newSegmentsCounter(segLen, nil, recordSegmentsDurations, nil)
	filters := pktque.Filters{sc}
	src := &pktque.FilterDemuxer{Demuxer: file, Filter: filters}
	var streams []av.CodecData
	var videoidx, audioidx int
	if streams, err = src.Streams(); err != nil {
		glog.Fatal("Can't count segments in source file")
	}
	for i, st := range streams {
		if st.Type().IsAudio() {
			audioidx = i
		}
		if st.Type().IsVideo() {
			videoidx = i
		}
	}
	glog.Infof("Video index: %d, audio index: %d", videoidx, audioidx)
	for {
		var pkt av.Packet
		if pkt, err = src.ReadPacket(); err != nil {
			if err == io.EOF {
				break
			}
			glog.Infof("Paket time %s", pkt.Time)
			sc.ModifyPacket(&pkt, streams, videoidx, audioidx)
		}
	}
	if !recordSegmentsDurations {
		return sc.segments - 1
	}
	return sc.SegmentsNeededForDuration(streamDuration)
}

func readAll(nc net.Conn) {
	var n int
	var err error
	eb := make([]byte, 1024)
	for {
		nc.SetReadDeadline(time.Now().Add(1 * time.Millisecond))
		n, err = nc.Read(eb)
		if err != nil || n < len(eb) {
			break
		}
	}
}

func (rs *rtmpStreamer) startUpload(fn, rtmpURL string, segmentsToStream int) {
	var err error
	rs.file, err = avutil.Open(fn)
	if err != nil {
		glog.Fatal(err)
	}
	rs.active = true
	defer func() {
		rs.active = false
	}()

	// pio.RecommendBufioSize = 1024 * 8
	// rtmp.Debug = true
	// rtmp.Debug2 = true
	// conn, err := rtmp.Dial("rtmp://localhost:1935/" + manifestID)
	conn, err := rtmp.Dial(rtmpURL)
	if err != nil {
		glog.Fatal(err)
	}

	var onError = func(err error) {
		glog.Error(err)
		rs.connectionLost = true
		rs.file.Close()
		conn.Close()
		time.Sleep(4 * time.Second)
		close(rs.done)
		return
	}

	// filters := pktque.Filters{&pktque.Walltime{}, &printKeyFrame{}, rs.counter}
	filters := pktque.Filters{rs.counter, &printKeyFrame{}, &pktque.FixTime{MakeIncrement: true}, &pktque.Walltime{}}

	demuxer := &pktque.FilterDemuxer{Demuxer: rs.file, Filter: filters}

	var streams []av.CodecData
	if streams, err = demuxer.Streams(); err != nil {
		onError(err)
		return
	}
	if err = conn.WriteHeader(streams); err != nil {
		onError(err)
		return
	}
	var doneStreaming bool
	for {
		lastSegments := 0
		for {
			var pkt av.Packet
			if pkt, err = demuxer.ReadPacket(); err != nil {
				if err != io.EOF {
					onError(err)
					return
				}
				if rs.counter.segments-rs.skippedSegments >= segmentsToStream {
					doneStreaming = true
				}
				break
			}
			if err = conn.WritePacket(pkt); err != nil {
				onError(err)
				return
			}
			if rs.counter.segments > lastSegments {
				// glog.Infof("rs.counter.segments: %d currentSegments: %d rs.skippedSegments: %d segmentsToStream: %d", rs.counter.segments, rs.counter.currentSegments, rs.skippedSegments, segmentsToStream)
				// fmt.Printf("rs.counter.segments: %d currentSegments: %d rs.skippedSegments: %d segmentsToStream: %d\n\n", rs.counter.segments, rs.counter.currentSegments, rs.skippedSegments, segmentsToStream)
				lastSegments = rs.counter.segments
			}
			if rs.counter.segments-rs.skippedSegments > segmentsToStream {
				glog.Info("Done streaming\n")
				// fmt.Println("=====!!! DONE streaming")
				doneStreaming = true
				rs.counter.segments--
				break
			}
		}
		if doneStreaming {
			break
		}
		// glog.Infof("=== REOPENING file!")
		// re-open same file and stream it again
		rs.counter.timeShift = rs.counter.lastPacketTime
		rs.file, err = avutil.Open(fn)
		if err != nil {
			glog.Fatal(err)
		}
		demuxer.Demuxer = rs.file
		// rs.counter.currentSegments = 0
		demuxer.Streams()
	}

	/*
		if err = avutil.CopyPackets(conn, demuxer); err != nil {
			if err != io.EOF {
				onError(err)
				return
			}
		}
	*/
	if err = conn.WriteTrailer(); err != nil {
		onError(err)
		return
	}

	/*
		err = avutil.CopyFile(conn, demuxer)
		if err != nil {
			glog.Error(err)
			rs.connectionLost = true
			rs.file.Close()
			conn.Close()
			time.Sleep(4 * time.Second)
			close(rs.done)
			return
		}
	*/

	rs.file.Close()
	glog.Infof("Waiting before closing RTMP stream\n")
	// fmt.Println("==== waiting before closing RTMP stream\n")
	// wait before closing connection, so we can recieve transcoded data
	// if we do not wait, last segment will be thrown out by broadcaster
	// with 'Session ended` error
	time.Sleep(8 * time.Second)
	// glog.Infof("---------- calling connection close rxbytes %d", conn.RxBytes())
	// nc := conn.NetConn()
	readAll(conn.NetConn())
	// pckt, err := conn.ReadPacket()
	// glog.Info("pck, err", pckt, err)
	err = conn.Close()
	// glog.Info("---------- calling connection close DONE", err)
	// time.Sleep(8 * time.Second)
	close(rs.done)
}
