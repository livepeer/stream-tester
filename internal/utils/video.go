package utils

import (
	"bytes"
	"fmt"
	"io"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/joy4/format/ts"
	"github.com/livepeer/stream-tester/internal/model"
)

// GetVideoStartTime returns timestamp of first frame of `ts` segment
func GetVideoStartTime(segment []byte) (time.Duration, error) {
	r := bytes.NewReader(segment)
	demuxer := ts.NewDemuxer(r)
	var videoIdx int8
	if strms, err := demuxer.Streams(); err == nil {
		// glog.V(model.VERBOSE).Infof("=======--- streams: %+v", strms)
		// glog.Infof("=======--- streams: %+v", strms)
		for i, s := range strms {
			if s == nil {
				continue
			}
			if s.Type().IsVideo() {
				videoIdx = int8(i)
				break
			}
		}
	} else {
		glog.Error("Error reading streams ", err)
		return 0, err
	}
	// glog.Infof("== Video index is %d", videoIdx)

	for {
		pkt, err := demuxer.ReadPacket()
		if err != nil {
			if err == io.EOF {
				break
			}
			glog.Error("Error reading packet", err)
			return 0, err
		}
		// glog.Infof("Packet idx %d key %v time %s\n", pkt.Idx, pkt.IsKeyFrame, pkt.Time)
		if pkt.Idx == videoIdx {
			glog.V(model.VERBOSE).Infof("=====--- first video paket idx %d, video idx %d, time %s", pkt.Idx, videoIdx, pkt.Time)
			// pktHash := md5.Sum(pkt.Data)
			// glog.Infof("=== downloaded hash of %s is %x", pkt.Time, pktHash)
			return pkt.Time, nil
		}
	}
	glog.Infof("No video packets found")
	return 0, fmt.Errorf("No video packets")
}

// GetVideoStartTimeAndDur ...
func GetVideoStartTimeAndDur(segment []byte) (time.Duration, time.Duration, error) {
	r := bytes.NewReader(segment)
	demuxer := ts.NewDemuxer(r)
	var videoIdx int8
	if strms, err := demuxer.Streams(); err == nil {
		// glog.V(model.VERBOSE).Infof("=======--- streams: %+v", strms)
		// glog.Infof("=======--- streams: %+v", strms)
		for i, s := range strms {
			if s == nil {
				continue
			}
			if s.Type().IsVideo() {
				videoIdx = int8(i)
				break
			}
		}
	} else {
		glog.Error("Error reading streams ", err)
		return 0, 0, err
	}
	// glog.Infof("== Video index is %d", videoIdx)

	var firstTime, lastTime time.Duration
	for {
		pkt, err := demuxer.ReadPacket()
		if err != nil {
			if err == io.EOF {
				break
			}
			glog.Error("Error reading packet", err)
			return 0, 0, err
		}
		// glog.Infof("Packet idx %d key %v time %s\n", pkt.Idx, pkt.IsKeyFrame, pkt.Time)
		// glog.Infof("=====--- first video paket idx %d, video idx %d, time %s is key %v is video %v", pkt.Idx, videoIdx, pkt.Time, pkt.IsKeyFrame, pkt.Idx == videoIdx)
		if pkt.Idx == videoIdx {
			// glog.V(model.VERBOSE).Infof("=====--- first video paket idx %d, video idx %d, time %s", pkt.Idx, videoIdx, pkt.Time)
			// pktHash := md5.Sum(pkt.Data)
			// glog.Infof("=== downloaded hash of %s is %x", pkt.Time, pktHash)
			if firstTime == 0 {
				glog.V(model.VERBOSE).Infof("=====--- first video paket idx %d, video idx %d, time %s is key %v is video %v", pkt.Idx, videoIdx, pkt.Time, pkt.IsKeyFrame, pkt.Idx == videoIdx)
				firstTime = pkt.Time
			}
		}
		lastTime = pkt.Time
	}
	return firstTime, lastTime - firstTime, nil
}
