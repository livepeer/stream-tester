package testers

import (
	"bytes"
	"context"
	"errors"
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
func StartSegmentingR(ctx context.Context, reader io.ReadSeekCloser, stopAtFileEnd bool, stopAfter, skipFirst, segLen time.Duration,
	useWallTime bool, out chan<- *model.HlsSegment) error {
	inFile, err := avutil.OpenRC(reader)
	if err != nil {
		glog.Errorf("avutil.OpenRC err=%v", err)
		return err
	}
	startSegmentingLoop(ctx, "", inFile, stopAtFileEnd, stopAfter, skipFirst, segLen, useWallTime, out)
	return nil
}

func StartSegmenting(ctx context.Context, fileName string, stopAtFileEnd bool, stopAfter, skipFirst, segLen time.Duration,
	useWallTime bool, out chan<- *model.HlsSegment) error {
	glog.Infof("Starting segmenting file %s", fileName)
	inFile, err := avutil.Open(fileName)
	if err != nil {
		glog.Errorf("avutil.OpenRC err=%v", err)
		return err
	}
	startSegmentingLoop(ctx, fileName, inFile, stopAtFileEnd, stopAfter, skipFirst, segLen, useWallTime, out)
	return nil
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
		delta := time.Until(pkttime)
		if delta > 0 {
			if wt.skipFirst == 0 || wt.skipFirst <= pkt.Time {
				time.Sleep(delta)
			}
		}
	}
	return
}

func startSegmentingLoop(ctx context.Context, fileName string, inFileReal av.DemuxCloser, stopAtFileEnd bool, stopAfter, skipFirst, segLen time.Duration,
	useWallTime bool, out chan<- *model.HlsSegment) {
	go func() {
		defer close(out)
		err := segmentingLoop(ctx, fileName, inFileReal, stopAtFileEnd, stopAfter, skipFirst, segLen, useWallTime, out)
		if err != nil {
			glog.Errorf("Error in segmenting loop. err=%+v", err)
			select {
			case out <- &model.HlsSegment{Err: err}:
			case <-ctx.Done():
			}
		}
	}()
}

func segmentingLoop(ctx context.Context, fileName string, inFileReal av.DemuxCloser,
	stopAtFileEnd bool, stopAfter, skipFirst, segLen time.Duration,
	useWallTime bool, out chan<- *model.HlsSegment) error {

	var err error
	var streams []av.CodecData
	streamTypes := map[int8]string{}

	ts := &timeShifter{}
	filters := pktque.Filters{ts, &pktque.FixTime{MakeIncrement: true}}
	if useWallTime {
		filters = append(filters, &Walltime{skipFirst: skipFirst})
	}
	inFile := &pktque.FilterDemuxer{Demuxer: inFileReal, Filter: filters}
	if streams, err = inFile.Streams(); err != nil {
		glog.Errorf("Can't get info about file err=%q, isNoAudio=%v isNoVideo=%v stack=%+v", err, errors.Is(err, jerrors.ErrNoAudioInfoFound), errors.Is(err, jerrors.ErrNoVideoInfoFound), err)
		return err
	}
	for i, st := range streams {
		codec := st.Type()
		ctype := "unknown"
		if codec.IsAudio() {
			ctype = "audio"
		} else if codec.IsVideo() {
			ctype = "video"
		}
		streamTypes[int8(i)] = ctype
	}
	glog.V(model.VERBOSE).Infof("Stream types=%+v", streamTypes)

	sendSegment := func(seg *model.HlsSegment) bool {
		select {
		case out <- &model.HlsSegment{Err: err}:
			return true
		case <-ctx.Done():
			return false
		}
	}

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
			return err
		}
		if firstFramePacket != nil {
			err = segFile.WritePacket(*firstFramePacket)
			if err != nil {
				return err
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
				// don't return ctx.Err() as that would send the error in the out channel
				return nil
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
				return rerr
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
				glog.V(model.VERBOSE).Infof("Packet isKeyframe=%v codecType=%v PTS=%s sincPrev=%s seqNo=%d", pkt.IsKeyFrame, streamTypes[pkt.Idx], pkt.Time,
					pkt.Time-prevPTS, seqNo+1)
				// prevPTS = pkt.Time
				curDur = pkt.Time - prevPTS
				break
			}
			err = segFile.WritePacket(pkt)
			if err != nil {
				return err
			}
		}
		err = segFile.WriteTrailer()
		if err != nil {
			return err
		}
		if rerr == io.EOF && stopAfter > 0 && (prevPTS+curDur) < stopAfter && len(fileName) > 0 {
			// re-open same file and stream it again
			firstFramePacket = nil
			ts.timeShift = lastPacket.Time + 30*time.Millisecond
			inf, err := avutil.Open(fileName)
			if err != nil {
				return err
			}
			defer inf.Close()
			inFile.Demuxer = inf
			// rs.counter.currentSegments = 0
			inFile.Streams()
			send := true
			if curDur <= 250*time.Millisecond {
				send = false
			}
			glog.V(model.VVERBOSE).Infof("Wrapping segments seqNo=%d pts=%s dur=%s sending=%v", seqNo, prevPTS, curDur, send)
			if send {
				hlsSeg := &model.HlsSegment{
					// err:      rerr,
					SeqNo:    seqNo,
					Pts:      prevPTS,
					Duration: curDur,
					Data:     buf.Bytes(),
				}
				if !sendSegment(hlsSeg) {
					return nil
				}
				prevPTS = lastPacket.Time
			} else {
				seqNo--
			}
		} else {
			sent := -1
			// if rerr == nil || rerr == io.EOF && curDur > 250*time.Millisecond {
			// Do not send last segment if it is too small.
			// Currently transcoding on Nvidia returns bad segment
			// if source segment is too short
			if rerr == nil || rerr == io.EOF {
				if skipFirst == 0 || skipFirst < prevPTS {
					hlsSeg := &model.HlsSegment{
						// err:      rerr,
						SeqNo:    seqNo,
						Pts:      prevPTS,
						Duration: curDur,
						Data:     buf.Bytes(),
					}
					if !sendSegment(hlsSeg) {
						return nil
					}
					sent = 0
				}
			}

			if rerr == io.EOF {
				// hlsSeg := &hlsSegment{
				// 	err:   rerr,
				// 	seqNo: seqNo,
				// }
				// out <- hlsSeg
				hlsSeg := &model.HlsSegment{
					Err:   io.EOF,
					SeqNo: seqNo + 1 + sent,
					Pts:   prevPTS + curDur,
				}
				if !sendSegment(hlsSeg) {
					return nil
				}
				break
			}
		}
		if stopAfter > 0 && (prevPTS+curDur) > stopAfter {
			hlsSeg := &model.HlsSegment{
				Err:   io.EOF,
				SeqNo: seqNo + 1,
				Pts:   prevPTS + curDur,
			}
			if !sendSegment(hlsSeg) {
				return nil
			}
			break
		}
		seqNo++
	}
	return nil
}
