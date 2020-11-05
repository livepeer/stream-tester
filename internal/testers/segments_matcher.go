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
		sentFrames []sentFrameInfo
		mu         *sync.Mutex
		reqs       int64
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

func (sm *segmentsMatcher) frameSent(pkt av.Packet, isVideo bool) {
	sm.mu.Lock()
	sm.sentFrames = append(sm.sentFrames, sentFrameInfo{
		pts:        pkt.Time,
		sentAt:     time.Now(),
		isKeyFrame: pkt.IsKeyFrame,
		isVideo:    isVideo,
	})
	sm.mu.Unlock()
}

// matchSegment matches received segment to sent data
// returns latency and speed ratio
func (sm *segmentsMatcher) matchSegment(firstPaketsPTS time.Duration, segmentDuration time.Duration, receivedAt time.Time) (time.Duration, float64, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	var lastPaket, curPacket sentFrameInfo
	var startInd int
	sentFrames := sm.sentFrames

	for i, pkt := range sentFrames {
		if pkt.pts == firstPaketsPTS {
			// got exact match!
			curPacket = pkt
			startInd = i
			break
		} else if pkt.pts > firstPaketsPTS {
			// msg := fmt.Sprintf("Diff between RTMP and HLS paket's PTS is %s, from prev packet is %s prev pkt %s HSL pkt %s next pkt %s",
			// 	pkt.pts-firstPaketsPTS, firstPaketsPTS-lastPaket.pts, lastPaket.String(), firstPaketsPTS, pkt.String())
			// glog.Info(msg)
			// panic("stop")
			curPacket = pkt
			if firstPaketsPTS-lastPaket.pts < pkt.pts-firstPaketsPTS {
				curPacket = lastPaket
			}
			startInd = i
			break
		}
		lastPaket = pkt
	}
	glog.V(model.VERBOSE).Infof(`looking for %s cur packet: %s last packet %s startInd %d`, firstPaketsPTS, curPacket.String(), lastPaket.String(), startInd)
	// for i, pkt := range sm.sentFrames {
	// 	glog.Info(i, pkt.String())
	// }
	// panic("stop")
	if curPacket.sentAt.IsZero() {
		// not found
		// panic(fmt.Sprintf(`not found match for segment with %s PTS`, firstPaketsPTS))
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
	glog.V(model.VERBOSE).Infof(`last packet %s sent at %s received at %s latency %s`, lastPaket.String(), lastPaket.sentAt, receivedAt, latency)
	if atomic.AddInt64(&sm.reqs, 1)%16 == 0 {
		sm.cleanup()
	}
	return latency, float64(latency) / float64(segmentDuration), nil
}

func (sm *segmentsMatcher) cleanup() {
	glog.V(model.VVERBOSE).Infof(`segments matcher cleanup start len %d`, len(sm.sentFrames))
	// sm.mu.Lock()
	// glog.Infof(`segments matcher cleanup start len %d 2`, len(sm.sentFrames))
	defer func() {
		glog.V(model.VVERBOSE).Infof(`segments matcher cleanup start len %d end`, len(sm.sentFrames))
	}()
	// defer sm.mu.Unlock()
	if len(sm.sentFrames) == 0 {
		return
	}
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
}

func (sfi *sentFrameInfo) String() string {
	return fmt.Sprintf(`{pts %s is key %v is video %v}`, sfi.pts, sfi.isKeyFrame, sfi.isVideo)
}
