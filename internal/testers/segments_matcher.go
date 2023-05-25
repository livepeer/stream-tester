package testers

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/joy4/av"
	"github.com/livepeer/stream-tester/model"
)

type (

	/*
		segmentsMatcher record timings of frames sent by RTMP
		matches segments that are read back
	*/
	segmentsMatcher struct {
		sentFrames    []sentFrameInfo
		sentFramesNum int64
		mu            *sync.Mutex
		reqs          int64
		firstPTS      time.Duration
		lastPTS       time.Duration
		lastPTS1      time.Duration
		lastPTS2      time.Duration
	}

	sentFrameInfo struct {
		pts        time.Duration
		sentAt     time.Time
		isKeyFrame bool
		isVideo    bool
	}
)

func newSegmentsMatcher() *segmentsMatcher {
	return &segmentsMatcher{
		sentFrames: make([]sentFrameInfo, 0, 1024),
		mu:         &sync.Mutex{},
	}
}

func (sm *segmentsMatcher) getStartEnd() (time.Duration, time.Duration, time.Duration, time.Duration) {
	sm.mu.Lock()
	f, l2, l1, l := sm.firstPTS, sm.lastPTS2, sm.lastPTS1, sm.lastPTS
	sm.mu.Unlock()
	return f, l2, l1, l
}

func (sm *segmentsMatcher) frameSent(pkt av.Packet, isVideo bool) {
	sm.mu.Lock()
	if isVideo && pkt.IsKeyFrame {
		if sm.sentFramesNum == 0 {
			sm.firstPTS = pkt.Time
		}
		if pkt.Time > sm.lastPTS {
			sm.lastPTS2 = sm.lastPTS1
			sm.lastPTS1 = sm.lastPTS
			sm.lastPTS = pkt.Time
		}
		sm.sentFrames = append(sm.sentFrames, sentFrameInfo{
			pts:        pkt.Time,
			sentAt:     time.Now(),
			isKeyFrame: pkt.IsKeyFrame,
			isVideo:    isVideo,
		})
	}
	sm.sentFramesNum++
	sm.mu.Unlock()
}

// matchSegment matches received segment to sent data
// returns latency and speed ratio
func (sm *segmentsMatcher) matchSegment(firstPaketsPTS time.Duration, segmentDuration time.Duration, receivedAt time.Time) (time.Duration, float64, error) {
	var lastPaket, curPacket sentFrameInfo
	var startInd int
	sm.mu.Lock()
	sentFrames := sm.sentFrames
	sm.mu.Unlock()

	for i, pkt := range sentFrames {
		if pkt.pts == firstPaketsPTS {
			// got exact match!
			curPacket = pkt
			startInd = i
			break
		} else if pkt.pts > firstPaketsPTS {
			curPacket = pkt
			if firstPaketsPTS-lastPaket.pts < pkt.pts-firstPaketsPTS {
				curPacket = lastPaket
			}
			startInd = i
			break
		}
		lastPaket = pkt
	}
	glog.V(model.VVERBOSE).Infof(`looking for %s cur packet: %s last packet %s startInd %d`, firstPaketsPTS, curPacket.String(), lastPaket.String(), startInd)
	if curPacket.sentAt.IsZero() {
		// not found
		return 0, 0, fmt.Errorf(`not found match for segment with %s PTS`, firstPaketsPTS)
	}
	for i := startInd; i < len(sentFrames); i++ {
		pkt := sentFrames[i]
		if pkt.pts > firstPaketsPTS+segmentDuration && pkt.isKeyFrame {
			break
		}
		lastPaket = pkt
	}
	latency := receivedAt.Sub(lastPaket.sentAt)
	glog.V(model.VVERBOSE).Infof(`last packet %s sent at %s received at %s latency %s`, lastPaket.String(), lastPaket.sentAt, receivedAt, latency)
	if atomic.AddInt64(&sm.reqs, 1)%16 == 0 {
		sm.cleanup()
	}
	return latency, float64(latency) / float64(segmentDuration), nil
}

func (sm *segmentsMatcher) cleanup() {
	glog.V(model.VVERBOSE).Infof(`segments matcher cleanup start len %d`, len(sm.sentFrames))
	defer func() {
		glog.V(model.VVERBOSE).Infof(`segments matcher cleanup start len %d end`, len(sm.sentFrames))
	}()
	if len(sm.sentFrames) == 0 {
		return
	}
	sm.mu.Lock()
	var until int
	now := time.Now()
	for i := len(sm.sentFrames) - 1; i >= 0; i-- {
		pkt := sm.sentFrames[i]
		if now.Sub(pkt.sentAt) > 3*60*time.Second {
			until = i
			break
		}
	}
	glog.V(model.VVERBOSE).Infof(`segments matcher cleanup  len %d until %d`, len(sm.sentFrames), until)
	if until > 0 {
		sm.sentFrames = sm.sentFrames[until:]
	}
	sm.mu.Unlock()
}

func (sfi *sentFrameInfo) String() string {
	return fmt.Sprintf(`{pts %s is key %v is video %v}`, sfi.pts, sfi.isKeyFrame, sfi.isVideo)
}
