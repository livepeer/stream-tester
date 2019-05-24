package utils

import (
	"bytes"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/stream-tester/internal/model"
	"github.com/nareix/joy4/format/ts"
)

// GetVideoStartTime returns timestamp of first frame of `ts` segment
func GetVideoStartTime(segment []byte) (time.Duration, error) {
	r := bytes.NewReader(segment)
	demuxer := ts.NewDemuxer(r)
	var videoIdx int8
	if strms, err := demuxer.Streams(); err == nil {
		glog.V(model.VERBOSE).Infof("=======--- streams: %+v", strms)
		for i, s := range strms {
			if s.Type().IsVideo() {
				videoIdx = int8(i)
				break
			}
		}
	} else {
		return 0, err
	}

	for {
		pkt, err := demuxer.ReadPacket()
		if err != nil {
			return 0, err
		}
		if pkt.Idx == videoIdx {
			glog.V(model.VERBOSE).Infof("=====--- first video paket idx %d, video idx %d, time %s", pkt.Idx, videoIdx, pkt.Time)
			return pkt.Time, nil
		}
	}
}
