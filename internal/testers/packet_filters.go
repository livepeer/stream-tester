package testers

import (
	"time"

	"../model"
	"github.com/golang/glog"
	"github.com/nareix/joy4/av"
)

type printKeyFrame struct {
}

func (pkf *printKeyFrame) ModifyPacket(pkt *av.Packet, streams []av.CodecData, videoidx int, audioidx int) (drop bool, err error) {
	if pkt.Idx == int8(videoidx) && pkt.IsKeyFrame {
		glog.V(model.VERBOSE).Infof("====== Got keyframe idx %d time %s composition time %s\n", pkt.Idx, pkt.Time, pkt.CompositionTime)
	}
	return
}

type segmentsCounter struct {
	segLen             time.Duration
	segments           int
	lastKeyUsedKeyTime time.Duration
}

func (sc *segmentsCounter) ModifyPacket(pkt *av.Packet, streams []av.CodecData, videoidx int, audioidx int) (drop bool, err error) {
	if pkt.Idx == int8(videoidx) && pkt.IsKeyFrame {
		// if pkt.Time-sc.lastKeyUsedKeyTime >= sc.segLen {
		// glog.V(model.VERBOSE).Infof("====== Number of segments: %d time %s last time %s diff %s\n", sc.segments,
		// 	pkt.Time, sc.lastKeyUsedKeyTime, pkt.Time-sc.lastKeyUsedKeyTime)
		if pkt.Time-sc.lastKeyUsedKeyTime > sc.segLen {
			sc.segments++
			glog.V(model.VERBOSE).Infof("====== Number of segments: %d time %s last time %s diff %s\n", sc.segments,
				pkt.Time, sc.lastKeyUsedKeyTime, pkt.Time-sc.lastKeyUsedKeyTime)
			sc.lastKeyUsedKeyTime = pkt.Time
		}
	}
	return
}
