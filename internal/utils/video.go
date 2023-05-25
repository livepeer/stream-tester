package utils

import (
	"bytes"
	"errors"
	"fmt"
	"image"
	"image/jpeg"
	"io"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/joy4/format/ts"
	"github.com/livepeer/stream-tester/model"
)

// GetVideoStartTime returns timestamp of first frame of `ts` segment
func GetVideoStartTime(segment []byte) (time.Duration, error) {
	r := bytes.NewReader(segment)
	demuxer := ts.NewDemuxer(r)
	var videoIdx int8
	if strms, err := demuxer.Streams(); err == nil {
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

	for {
		pkt, err := demuxer.ReadPacket()
		if err != nil {
			if err == io.EOF {
				break
			}
			glog.Error("Error reading packet", err)
			return 0, err
		}
		if pkt.Idx == videoIdx {
			glog.V(model.VERBOSE).Infof("=====--- first video paket idx %d, video idx %d, time %s", pkt.Idx, videoIdx, pkt.Time)
			return pkt.Time, nil
		}
	}
	glog.Infof("No video packets found")
	return 0, fmt.Errorf("No video packets")
}

// GetVideoStartTimeAndDur ...
func GetVideoStartTimeAndDur(segment []byte) (time.Duration, time.Duration, error) {
	d1, d2, _, _, err := GetVideoStartTimeDurFrames(segment)
	return d1, d2, err
}

// GetVideoStartTimeDurFrames ...
func GetVideoStartTimeDurFrames(segment []byte) (time.Duration, time.Duration, int, []time.Duration, error) {
	var skeyframes []time.Duration
	r := bytes.NewReader(segment)
	demuxer := ts.NewDemuxer(r)
	var videoIdx int8 = -1
	var keyFrames int
	var lastKeyFramePTS time.Duration = -1
	if strms, err := demuxer.Streams(); err == nil {
		for i, s := range strms {
			if s == nil {
				continue
			}
			if s.Type().IsVideo() {
				if videoIdx == -1 {
					videoIdx = int8(i)
				} else {
					glog.Error("Multiple video streams found")
					return 0, 0, 0, nil, errors.New("multiple video streams founds")
				}
			}
		}
	} else {
		glog.Error("Error reading streams ", err)
		return 0, 0, 0, nil, err
	}

	var lastTime, oneFrameDiff time.Duration
	var firstTime time.Duration = -1

	for {
		pkt, err := demuxer.ReadPacket()
		if err != nil {
			if err == io.EOF {
				break
			}
			glog.Error("Error reading packet", err)
			return 0, 0, 0, nil, err
		}
		if pkt.Idx == videoIdx {
			if pkt.IsKeyFrame {
				if lastKeyFramePTS == -1 || pkt.Time != lastKeyFramePTS {
					keyFrames++
					lastKeyFramePTS = pkt.Time
					skeyframes = append(skeyframes, pkt.Time)
				}
			}
			if firstTime == -1 {
				glog.V(model.INSANE).Infof("=====--- first video paket idx %d, video idx %d, time %s is key %v is video %v", pkt.Idx, videoIdx, pkt.Time, pkt.IsKeyFrame, pkt.Idx == videoIdx)
				firstTime = pkt.Time
			} else if oneFrameDiff == 0 {
				oneFrameDiff = pkt.Time - firstTime
			}
			lastTime = pkt.Time
		}
	}
	return firstTime, lastTime - firstTime + oneFrameDiff, keyFrames, skeyframes, nil
}

// Img2Jpeg encodees img to jpeg
func Img2Jpeg(img *image.YCbCr) []byte {
	w := new(bytes.Buffer)
	jpeg.Encode(w, img, nil)
	return w.Bytes()
}
