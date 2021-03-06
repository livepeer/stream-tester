package testers

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/joy4/av"
	"github.com/livepeer/joy4/av/avutil"
	"github.com/livepeer/joy4/av/pktque"
	"github.com/livepeer/joy4/format/ts"
	"github.com/livepeer/joy4/jerrors"
	"github.com/livepeer/stream-tester/model"
)

// segmenter take video file and cuts it into .ts segments
// type segmenter struct {
// 	stopAtFileEnd bool
// }

// func newSegmenter(stopAtFileEnd bool) *segmenter {
// 	return &segmenter{
// 		stopAtFileEnd: stopAtFileEnd,
// 	}
// }

type hlsSegment struct {
	err      error
	seqNo    int
	pts      time.Duration
	duration time.Duration
	data     []byte
}

func startSegmenting(ctx context.Context, fileName string, stopAtFileEnd bool, stopAfter, skipFirst time.Duration, out chan<- *hlsSegment) error {
	glog.Infof("Starting segmenting file %s", fileName)
	inFile, err := avutil.Open(fileName)
	if err != nil {
		glog.Fatal(err)
	}
	go segmentingLoop(ctx, fileName, inFile, stopAtFileEnd, stopAfter, skipFirst, out)

	return err
}

func createInMemoryTSMuxer() (av.Muxer, *bytes.Buffer) {
	// write muxer
	// "github.com/livepeer/joy4/format/ts"
	// ts.NewMuxer()
	// w := bytes.NewBuffer()
	// ts.Handler()
	// buf := new(buffer)
	// var w io.WriteCloser = buf
	// if w, err = self.createUrl(u, uri); err != nil {
	// 	return
	// }
	// muxer := &avutil.HandlerMuxer{
	// 	Muxer: ts.NewMuxer(w),
	// 	w:     w,
	// }
	// return muxer
	buf := new(bytes.Buffer)
	return ts.NewMuxer(buf), buf
}

// Walltime make packets reading speed as same as walltime, effect like ffmpeg -re option.
type Walltime struct {
	firsttime time.Time
	skipFirst time.Duration
}

// ModifyPacket public filter's interface
func (wt *Walltime) ModifyPacket(pkt *av.Packet, streams []av.CodecData, videoidx int, audioidx int) (drop bool, err error) {
	if pkt.Idx == 0 {
		if wt.firsttime.IsZero() {
			wt.firsttime = time.Now()
			if wt.skipFirst > 0 {
				wt.firsttime = wt.firsttime.Add(-wt.skipFirst)
			}
		}
		pkttime := wt.firsttime.Add(pkt.Time)
		delta := pkttime.Sub(time.Now())
		if delta > 0 {
			if wt.skipFirst == 0 || wt.skipFirst <= pkt.Time {
				time.Sleep(delta)
			}
		}
	}
	return
}

func segmentingLoop(ctx context.Context, fileName string, inFileReal av.DemuxCloser, stopAtFileEnd bool, stopAfter, skipFirst time.Duration, out chan<- *hlsSegment) {
	var err error
	var streams []av.CodecData
	var videoidx, audioidx int8

	ts := &timeShifter{}
	filters := pktque.Filters{ts, &pktque.FixTime{MakeIncrement: true}, &Walltime{skipFirst: skipFirst}}
	inFile := &pktque.FilterDemuxer{Demuxer: inFileReal, Filter: filters}
	if streams, err = inFile.Streams(); err != nil {
		msg := fmt.Sprintf("Can't get info about file: '%+v', isNoAudio %v isNoVideo %v", err, errors.Is(err, jerrors.ErrNoAudioInfoFound), errors.Is(err, jerrors.ErrNoVideoInfoFound))
		if !(errors.Is(err, jerrors.ErrNoAudioInfoFound) || errors.Is(err, jerrors.ErrNoVideoInfoFound)) {
			glog.Fatal(msg)
		}
		fmt.Println(msg)
		panic(msg)
	}
	for i, st := range streams {
		if st.Type().IsAudio() {
			audioidx = int8(i)
		}
		if st.Type().IsVideo() {
			videoidx = int8(i)
		}
	}
	glog.V(model.VERBOSE).Infof("Video stream index %d, audio stream index %d\n", videoidx, audioidx)

	seqNo := 0
	// var curPTS time.Duration
	var firstFramePacket *av.Packet
	var lastPacket av.Packet
	var prevPTS, curDur time.Duration
	for {
		// segName := fmt.Sprintf("%d.ts", seqNo)
		// segFile, err := avutil.Create(segName)
		// if err != nil {
		// 	glog.Fatal(err)
		// }
		segFile, buf := createInMemoryTSMuxer()
		err = segFile.WriteHeader(streams)
		if err != nil {
			glog.Fatal(err)
		}
		if firstFramePacket != nil {
			err = segFile.WritePacket(*firstFramePacket)
			if err != nil {
				glog.Fatal(err)
			}
			prevPTS = firstFramePacket.Time
			firstFramePacket = nil
		}
		// var curSegStart = curPTS
		var rerr error
		var pkt av.Packet
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			pkt, rerr = inFile.ReadPacket()
			if rerr != nil {
				if rerr == io.EOF {
					if lastPacket.Time != 0 {
						curDur = lastPacket.Time - prevPTS
					}
					break
				}
				glog.Fatal(rerr)
			}
			lastPacket = pkt

			// fmt.Printf("Packet Is Keyframe %v Is Audio %v Is Video %v PTS %s\n", pkt.IsKeyFrame, pkt.Idx == audioidx, pkt.Idx == videoidx, pkt.Time)
			// curPTS = pkt.Time
			// if curPTS-curSegStart > 1900*time.Millisecond && pkt.IsKeyFrame {
			// 	firstFramePacket = &pkt
			// 	break
			// }
			// This matches segmenter algorithm used in ffmpeg
			if pkt.IsKeyFrame && pkt.Time >= time.Duration(seqNo+1)*segLen {
				firstFramePacket = &pkt
				glog.V(model.VERBOSE).Infof("Packet Is Keyframe %v Is Audio %v Is Video %v PTS %s sinc prev %s seqNo %d\n", pkt.IsKeyFrame, pkt.Idx == audioidx, pkt.Idx == videoidx, pkt.Time,
					pkt.Time-prevPTS, seqNo+1)
				// prevPTS = pkt.Time
				curDur = pkt.Time - prevPTS
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
		if rerr == io.EOF && stopAfter > 0 && (prevPTS+curDur) < stopAfter {
			// re-open same file and stream it again
			firstFramePacket = nil
			ts.timeShift = lastPacket.Time + 30*time.Millisecond
			inf, err := avutil.Open(fileName)
			if err != nil {
				glog.Fatal(err)
			}
			inFile.Demuxer = inf
			// rs.counter.currentSegments = 0
			inFile.Streams()
			send := true
			if curDur <= 250*time.Millisecond {
				send = false
			}
			glog.V(model.VVERBOSE).Infof("Wrapping segments seqNo=%d pts=%s dur=%s sending=%v", seqNo, prevPTS, curDur, send)
			if send {
				hlsSeg := &hlsSegment{
					// err:      rerr,
					seqNo:    seqNo,
					pts:      prevPTS,
					duration: curDur,
					data:     buf.Bytes(),
				}
				out <- hlsSeg
				prevPTS = lastPacket.Time
			} else {
				seqNo--
			}
		} else {
			sent := -1
			if rerr == nil || rerr == io.EOF && curDur > 250*time.Millisecond {
				// Do not send last segment if it is too small.
				// Currently transcoding on Nvidia returns bad segment
				// if source segment is too short
				if skipFirst == 0 || skipFirst < prevPTS {
					hlsSeg := &hlsSegment{
						// err:      rerr,
						seqNo:    seqNo,
						pts:      prevPTS,
						duration: curDur,
						data:     buf.Bytes(),
					}
					out <- hlsSeg
					sent = 0
				}
			}

			if rerr == io.EOF {
				// hlsSeg := &hlsSegment{
				// 	err:   rerr,
				// 	seqNo: seqNo,
				// }
				// out <- hlsSeg
				hlsSeg := &hlsSegment{
					err:   io.EOF,
					seqNo: seqNo + 1 + sent,
					pts:   prevPTS + curDur,
				}
				out <- hlsSeg
				break
			}
		}
		if stopAfter > 0 && (prevPTS+curDur) > stopAfter {
			hlsSeg := &hlsSegment{
				err:   io.EOF,
				seqNo: seqNo + 1,
				pts:   prevPTS + curDur,
			}
			out <- hlsSeg
			break
		}
		seqNo++
	}
	return
}
