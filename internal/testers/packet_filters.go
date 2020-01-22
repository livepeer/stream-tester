package testers

import (
	"time"

	"github.com/golang/glog"
	"github.com/gosuri/uiprogress"
	"github.com/livepeer/joy4/av"
	"github.com/livepeer/stream-tester/model"
	"github.com/livepeer/stream-tester/internal/utils"
)

type printKeyFrame struct {
	lastTime time.Duration
}

func (pkf *printKeyFrame) ModifyPacket(pkt *av.Packet, streams []av.CodecData, videoidx int, audioidx int) (drop bool, err error) {
	// if pkt.Idx == int8(videoidx) || true {
	// 	glog.Infof("====== videoidx %d isKey %v Got frame idx %d time %s comp time %s \n", videoidx, pkt.IsKeyFrame, pkt.Idx, pkt.Time, pkt.CompositionTime)
	// }
	if pkt.Idx == int8(videoidx) && pkt.IsKeyFrame {
		diff := pkt.Time - pkf.lastTime
		glog.V(model.VERBOSE).Infof("====== Got keyframe idx %d time %s diff %s\n", pkt.Idx, pkt.Time, diff)
		pkf.lastTime = pkt.Time
	}
	return
}

// timeShifter just shift time by specified duration
// used when streaming same file in loop
type timeShifter struct {
	timeShift time.Duration
}

func (ts *timeShifter) ModifyPacket(pkt *av.Packet, streams []av.CodecData, videoidx int, audioidx int) (drop bool, err error) {
	pkt.Time += ts.timeShift
	return
}

// segmentsCounter counts segments by counting key frames
// with same algorithm used by ffmeg to cut RTMP streams into HLS segments
type segmentsCounter struct {
	segLen                  time.Duration
	currentSegments         int
	segments                int
	segmentsStartTimes      []time.Duration
	lastKeyUsedKeyTime      time.Duration
	bar                     *uiprogress.Bar
	recordSegmentsDurations bool
	segmentsDurations       []time.Duration
	timeShift               time.Duration
	lastPacketTime          time.Duration
	saveSentTimes           bool
	lastSentSegmentTime     time.Duration
	sentTimes               []time.Time
	sentTimesMap            *utils.SyncedTimesMap
}

func newSegmentsCounter(segLen time.Duration, bar *uiprogress.Bar, recordSegmentsDurations bool, sentTimesMap *utils.SyncedTimesMap) *segmentsCounter {
	return &segmentsCounter{
		bar:                     bar,
		segLen:                  segLen,
		recordSegmentsDurations: recordSegmentsDurations,
		sentTimesMap:            sentTimesMap,
		segmentsStartTimes:      make([]time.Duration, 10, 10), // Record segments start timestamps. Needed
		// to detect how many segments broadcaster skipped st start
	}
}

func (sc *segmentsCounter) ModifyPacket(pkt *av.Packet, streams []av.CodecData, videoidx int, audioidx int) (drop bool, err error) {
	pkt.Time += sc.timeShift
	if pkt.Idx == int8(videoidx) && pkt.IsKeyFrame {
		// pktHash := md5.Sum(pkt.Data)
		// glog.Infof("=== hash of %s is %x", pkt.Time, pktHash)
		// This matches segmenter algorithm used in ffmpeg
		if pkt.Time >= time.Duration(sc.currentSegments+1)*sc.segLen {
			if sc.segments < len(sc.segmentsStartTimes) {
				sc.segmentsStartTimes[sc.segments] = pkt.Time
			}
			if sc.sentTimesMap != nil {
				now := time.Now()
				// sc.sentTimes = append(sc.sentTimes, now)
				// glog.Infof("=== sent seqNo %d sent time %s pkt time %s", sc.segments, now, sc.lastSentSegmentTime)
				sc.sentTimesMap.SetTime(sc.lastSentSegmentTime, now)
				sc.lastSentSegmentTime = pkt.Time
			}
			sc.segments++
			sc.currentSegments++
			if sc.bar != nil {
				sc.bar.Incr()
			}
			if sc.recordSegmentsDurations {
				sc.segmentsDurations = append(sc.segmentsDurations, pkt.Time-sc.lastKeyUsedKeyTime)
			}
			glog.V(model.VERBOSE).Infof("====== Number of segments: %d current segments: %d time %s last time %s diff %s data size %d time shift: %s\n",
				sc.segments, sc.currentSegments, pkt.Time, sc.lastKeyUsedKeyTime, pkt.Time-sc.lastKeyUsedKeyTime, len(pkt.Data), sc.timeShift)
			sc.lastKeyUsedKeyTime = pkt.Time
		}
	}
	sc.lastPacketTime = pkt.Time
	return
}

func (sc *segmentsCounter) SegmentsNeededForDuration(targetDuration time.Duration) int {
	tot := sc.totalDuration()
	repeats := int(targetDuration / tot)
	targetLeft := targetDuration % tot
	var left time.Duration
	glog.Infof("total: %s repeats: %d targetLeft: %s", tot, repeats, targetLeft)
	for i, d := range sc.segmentsDurations {
		left += d
		if left >= targetLeft {
			return repeats*len(sc.segmentsDurations) + i
		}
	}
	// shouldn't get here
	return repeats * len(sc.segmentsDurations)
}

func (sc *segmentsCounter) totalDuration() time.Duration {
	var tot time.Duration
	for _, d := range sc.segmentsDurations {
		tot += d
	}
	return tot
}
