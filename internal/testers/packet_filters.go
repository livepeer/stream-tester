package testers

import (
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/stream-tester/internal/model"
	"github.com/nareix/joy4/av"
)

type printKeyFrame struct {
	lastTime time.Duration
}

func (pkf *printKeyFrame) ModifyPacket(pkt *av.Packet, streams []av.CodecData, videoidx int, audioidx int) (drop bool, err error) {
	if pkt.Idx == int8(videoidx) && pkt.IsKeyFrame {
		diff := pkt.Time - pkf.lastTime
		glog.V(model.VERBOSE).Infof("====== Got keyframe idx %d time %s diff %s\n", pkt.Idx, pkt.Time, diff)
		pkf.lastTime = pkt.Time
	}
	return
}

// segmentsCounter counts segments by counting key frames
// with same algorithm used by ffmeg to cut RTMP streams into HLS segments
type segmentsCounter struct {
	segLen             time.Duration
	segments           int
	segmentsStartTimes []time.Duration
	lastKeyUsedKeyTime time.Duration
}

func newSegmentsCounter(segLen time.Duration) *segmentsCounter {
	return &segmentsCounter{
		segLen:             segLen,
		segmentsStartTimes: make([]time.Duration, 10, 10), // Record segments start timestamps. Needed
		// to detect how many segments broadcaster skipped st start
	}
}

func (sc *segmentsCounter) ModifyPacket(pkt *av.Packet, streams []av.CodecData, videoidx int, audioidx int) (drop bool, err error) {
	if pkt.Idx == int8(videoidx) && pkt.IsKeyFrame {
		// This matches segmenter algorithm used in ffmpeg
		if pkt.Time >= time.Duration(sc.segments+1)*sc.segLen {
			if sc.segments < len(sc.segmentsStartTimes) {
				sc.segmentsStartTimes[sc.segments] = pkt.Time
			}
			sc.segments++
			glog.V(model.VERBOSE).Infof("====== Number of segments: %d time %s last time %s diff %s data size %d\n", sc.segments,
				pkt.Time, sc.lastKeyUsedKeyTime, pkt.Time-sc.lastKeyUsedKeyTime, len(pkt.Data))
			sc.lastKeyUsedKeyTime = pkt.Time
		}
	}
	return
}
