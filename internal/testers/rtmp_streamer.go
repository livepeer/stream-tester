package testers

import (
	"context"
	"fmt"
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
	"github.com/livepeer/stream-tester/messenger"
	"github.com/livepeer/stream-tester/model"
)

var segLen = 2 * time.Second

// rtmpStreamer streams one video file to RTMP server
type rtmpStreamer struct {
	ctx             context.Context
	cancel          context.CancelFunc
	baseManifestID  string
	ingestURL       string
	counter         *segmentsCounter
	skippedSegments int
	connectionLost  bool
	active          bool
	file            av.DemuxCloser
	wowzaMode       bool
	segmentsMatcher *segmentsMatcher
	hasBar          bool
	started         time.Time
}

// NewRtmpStreamer ...
func NewRtmpStreamer(ctx context.Context, cancel context.CancelFunc, ingestURL string) *rtmpStreamer {
	return &rtmpStreamer{
		ctx:       ctx,
		cancel:    cancel,
		ingestURL: ingestURL,
		counter:   newSegmentsCounter(segLen, nil, false, nil),
	}
}

// source is local file name for now
func newRtmpStreamer(ctx context.Context, cancel context.CancelFunc, ingestURL, source, baseManifestID string,
	sentTimesMap *utils.SyncedTimesMap, bar *uiprogress.Bar, wowzaMode bool, sm *segmentsMatcher) *rtmpStreamer {

	return &rtmpStreamer{
		ctx:             ctx,
		cancel:          cancel,
		wowzaMode:       wowzaMode,
		ingestURL:       ingestURL,
		counter:         newSegmentsCounter(segLen, bar, false, sentTimesMap),
		hasBar:          bar != nil,
		baseManifestID:  baseManifestID,
		segmentsMatcher: sm,
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
		glog.Fatalf("Can't count segments in source file %+v", err)
	}
	for i, st := range streams {
		if st.Type().IsAudio() {
			audioidx = i
		}
		if st.Type().IsVideo() {
			videoidx = i
		}
	}
	glog.V(model.VERBOSE).Infof("Video index: %d, audio index: %d", videoidx, audioidx)
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

func chooseNeededStreams(streams []av.CodecData) (int8, int8, []av.CodecData) {
	audioidx := -1
	videoidx := -1
	needed := make([]av.CodecData, 0, 2)
	for i, strm := range streams {
		if strm == nil {
			continue
		}
		if strm.Type().IsVideo() {
			videoidx = i
			needed = append(needed, strm)
		}
		if strm.Type().IsAudio() {
			astrm := strm.(av.AudioCodecData)
			glog.V(model.VERBOSE).Infof("Audio stream %d type %v", i, astrm.Type())
			if astrm.Type() == av.AAC {
				audioidx = i
				needed = append(needed, strm)
			}
		}
	}
	if videoidx == -1 {
		panic("No video stream found.")
	}
	if audioidx == -1 {
		panic("No supported (AAC) audio stream found.")
	}
	return int8(audioidx), int8(videoidx), needed
}

func (rs *rtmpStreamer) Stop() {
	rs.file.Close()
}

// StartUpload starts RTMP stream. Blocks until end.
func (rs *rtmpStreamer) StartUpload(fn, rtmpURL string, streamDuration, waitForTarget time.Duration) {
	var err error
	var conn *rtmp.Conn
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
	// conn, err := rtmp.Dial(rtmpURL)
	started := time.Now()
	rs.started = started
	for {
		conn, err = rtmp.DialTimeout(rtmpURL, 4*time.Second)
		if err != nil {
			if waitForTarget > 0 {
				if time.Since(started) > waitForTarget {
					msg := fmt.Sprintf(`Can't connect to %s for %s`, rtmpURL, waitForTarget)
					fmt.Println(msg)
					messenger.SendFatalMessage(msg)
					rs.file.Close()
					// close(rs.done)
					rs.closeDone()
					return
				}
				time.Sleep(2 * time.Second)
				continue
			} else {
				glog.Fatal(err)
			}
		}
		break
	}

	var onError = func(err error) {
		msg := fmt.Sprintf("onError finishing upload : %v", err)
		messenger.SendFatalMessage(msg)
		glog.Error(msg)
		rs.connectionLost = true
		rs.file.Close()
		conn.Close()
		time.Sleep(4 * time.Second)
		rs.closeDone()
		return
	}

	// filters := pktque.Filters{&pktque.Walltime{}, &printKeyFrame{}, rs.counter}
	filters := pktque.Filters{rs.counter, &printKeyFrame{}, &pktque.FixTime{MakeIncrement: true}, &pktque.Walltime{}}

	demuxer := &pktque.FilterDemuxer{Demuxer: rs.file, Filter: filters}

	var rawStreams, streams []av.CodecData
	var audioidx, videoidx int8
	if rawStreams, err = demuxer.Streams(); err != nil {
		onError(err)
		return
	}
	glog.V(model.VERBOSE).Infof("=== Raw streams %d", len(rawStreams))
	audioidx, videoidx, streams = chooseNeededStreams(rawStreams)
	if err = conn.WriteHeader(streams); err != nil {
		onError(err)
		return
	}
outloop:
	for {
		lastSegments := 0
		var lastPacketTime time.Duration
		packetIdx := 0
		for {
			select {
			case <-rs.ctx.Done():
				glog.Infof("=========>>>> got stop singal")
				// rs.file.Close()
				// conn.Close()
				// return
				break outloop
			default:
			}
			var pkt av.Packet
			// glog.Infof("Reading packet %d", packetIdx)
			if pkt, err = demuxer.ReadPacket(); err != nil {
				if err != io.EOF {
					onError(err)
					return
				} else if rs.wowzaMode {
					// in Wowza mode can't really loop, just stopping at EOF
					glog.V(model.DEBUG).Info("==== RTMP streamer file ended.")
					break outloop
				}
				if streamDuration >= 0 && lastPacketTime >= streamDuration || streamDuration == 0 {
					break outloop
				}
				/*
					if segmentsToStream > 0 && rs.counter.segments-rs.skippedSegments >= segmentsToStream {
						break outloop
					}
				*/
				break
			}
			lastPacketTime = pkt.Time
			if pkt.Idx != audioidx && pkt.Idx != videoidx {
				continue
			}
			// glog.Infof("Writing packet %d pkt.Idx %d pkt.Time %s Composition Time %s stream duration %s", packetIdx, pkt.Idx, pkt.Time, pkt.CompositionTime, streamDuration)
			if streamDuration > 0 && pkt.IsKeyFrame && pkt.Idx == videoidx && pkt.Time >= streamDuration {
				conn.WritePacket(pkt)
				glog.Info("Done streaming\n")
				break outloop
			}
			start := time.Now()
			if err = conn.WritePacket(pkt); err != nil {
				onError(err)
				return
			}
			took := time.Since(start)
			if rs.segmentsMatcher != nil {
				rs.segmentsMatcher.frameSent(pkt, pkt.Idx == videoidx)
			}
			if took > 1000*time.Millisecond {
				glog.V(model.SHORT).Infof("packet %d writing took %s PTS %s rs.counter.segments: %d currentSegments: %d rs.skippedSegments: %d stream duration: %d", packetIdx, took,
					pkt.Time, rs.counter.segments, rs.counter.currentSegments, rs.skippedSegments, streamDuration)
			}
			if pkt.IsKeyFrame {
				glog.V(model.VERBOSE).Infof("sent keyframe PTS %s rs.counter.segments: %d idx %d is video %v", pkt.Time, rs.counter.segments, pkt.Idx, pkt.Idx == videoidx)
			}
			if rs.counter.segments > lastSegments {
				glog.V(model.VERBOSE).Infof("rs.counter.segments: %d currentSegments: %d rs.skippedSegments: %d PTS %s stream duration: %s",
					rs.counter.segments, rs.counter.currentSegments, rs.skippedSegments, pkt.Time, streamDuration)
				// glog.Infof("packet %d rs.counter.segments: %d currentSegments: %d rs.skippedSegments: %d segmentsToStream: %d", packetIdx, rs.counter.segments, rs.counter.currentSegments, rs.skippedSegments, segmentsToStream)
				// fmt.Printf("rs.counter.segments: %d currentSegments: %d rs.skippedSegments: %d segmentsToStream: %d\n\n", rs.counter.segments, rs.counter.currentSegments, rs.skippedSegments, segmentsToStream)
				lastSegments = rs.counter.segments
			}
			packetIdx++
		}
		glog.V(model.DEBUG).Infof("=== REOPENING file!")
		// re-open same file and stream it again
		rs.counter.timeShift = rs.counter.lastPacketTime + 30*time.Millisecond
		rs.file, err = avutil.Open(fn)
		if err != nil {
			glog.Fatal(err)
		}
		demuxer.Demuxer = rs.file
		demuxer.Streams()
	}

	glog.V(model.DEBUG).Info("Writing trailer")
	if err = conn.WriteTrailer(); err != nil {
		onError(err)
		return
	}

	rs.file.Close()
	// if rs.hasBar {
	// 	uiprogress.Stop()
	// }
	glog.V(model.DEBUG).Infof("Waiting before closing RTMP stream\n")
	// fmt.Println("==== waiting before closing RTMP stream\n")
	// wait before closing connection, so we can recieve transcoded data
	// if we do not wait, last segment will be thrown out by broadcaster
	// with 'Session ended` error
	time.Sleep(8 * time.Second)
	glog.V(model.DEBUG).Infof("---------- calling connection close rxbytes %d", conn.RxBytes())
	// nc := conn.NetConn()
	readAll(conn.NetConn())
	// pckt, err := conn.ReadPacket()
	// glog.Info("pck, err", pckt, err)
	glog.V(model.DEBUG).Info("---------- calling connection close start", err)
	err = conn.Close()
	glog.V(model.DEBUG).Info("---------- calling connection close DONE", err)
	// time.Sleep(8 * time.Second)
	rs.closeDone()
	glog.V(model.DEBUG).Info("---------- done channel closed", err)
}

func (rs *rtmpStreamer) closeDone() {
	rs.cancel()
}
