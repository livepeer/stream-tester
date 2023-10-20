//go:build h264
// +build h264

package codec

import (
	"bytes"
	"image"
	"io"

	"github.com/golang/glog"
	"github.com/livepeer/joy4/av"
	"github.com/livepeer/joy4/codec/h264parser"
	"github.com/livepeer/joy4/format/ts"
	"github.com/livepeer/stream-tester/model"
)

// TSFirstImage geets mpegts file and returns firstt frame as image
func TSFirstImage(tsb []byte) *image.YCbCr {
	// file, err := avutil.Open("hls/v108063.ts")
	// fmt.Printf("%T\n", file)
	file := ts.NewDemuxer(bytes.NewBuffer(tsb))
	file.NumStreamsToFind = 1
	/*
		hd := file.(*avutil.HandlerDemuxer)
		fmt.Printf("%T\n", hd.Demuxer)
		if err != nil {
			// panic(err)
			return nil
		}
	*/
	var err error
	var streams []av.CodecData
	var videoidx int
	if streams, err = file.Streams(); err != nil {
		glog.Infof("Error parsing segment %v", err)
		return nil
	}
	var vcd av.VideoCodecData
	for i, st := range streams {
		if st == nil {
			continue
		}
		if st.Type().IsVideo() {
			videoidx = i
		}
		if vc, ok := st.(av.VideoCodecData); ok {
			glog.V(model.DEBUG).Infof("w: %d, h: %d\n", vc.Width(), vc.Height())
			vcd = vc
		}
		if st.Type() == av.H264 {
			// vdec, err = ffmpeg.NewVideoDecoder(st)
			// if err != nil {
			// 	panic(err)
			// }
		}
	}
	hcd, ok := vcd.(h264parser.CodecData)
	if !ok {
		return nil
	}
	dec, err := NewH264Decoder(hcd.Record)
	if err != nil {
		return nil
	}
	for {
		pkt, err := file.ReadPacket()
		if err != nil {
			if err == io.EOF {
				break
			}
			glog.Infof("Error reading packet %v", err)
			return nil
		}
		if pkt.Idx == int8(videoidx) {
			img, err := dec.Decode(pkt.Data)
			if err != nil {
				glog.Info(err)
			} else {
				return img
			}
		}
	}

	return nil
}

/*
// Packet2Image ...
func Packet2Image(vcd av.VideoCodecData, pkt av.Packet) {
	hcd, ok := vcd.(h264parser.CodecData)
	if !ok {
		panic("stop")
	}
	dec, err := NewH264Decoder(hcd.Record)
	if err != nil {
		panic(err)
	}
	img, err := dec.Decode(pkt.Data)
	if err != nil {
		// panic(err)
		fmt.Println(err)
	} else {
		fmt.Println(img)
	}
}


// MTest ...
func MTest() {
	file, err := avutil.Open("hls/v108063.ts")
	fmt.Printf("%T\n", file)
	hd := file.(*avutil.HandlerDemuxer)
	fmt.Printf("%T\n", hd.Demuxer)
	if err != nil {
		panic(err)
	}
	// sc := newSegmentsCounter(segLen, nil, recordSegmentsDurations, nil)
	// filters := pktque.Filters{sc}
	// src := &pktque.FilterDemuxer{Demuxer: file, Filter: filters}
	var streams []av.CodecData
	var videoidx, audioidx int
	// var dec *ffmpeg.AudioDecoder
	// var vdec *ffmpeg.VideoDecoder
	if streams, err = file.Streams(); err != nil {
		fmt.Println("Can't count segments in source file")
		panic(err)
	}
	var vcd av.VideoCodecData
	for i, st := range streams {
		if st == nil {
			continue
		}
		fmt.Println(i, " stream is of type ", st.Type())
		if st.Type().IsAudio() {
			audioidx = i
		}
		if st.Type().IsVideo() {
			videoidx = i
		}
		if vc, ok := st.(av.VideoCodecData); ok {
			fmt.Printf("w: %d, h: %d\n", vc.Width(), vc.Height())
			vcd = vc
		}
		if st.Type() == av.H264 {
			// panic("good")
			// vdec, err = ffmpeg.NewVideoDecoder(st)
			// if err != nil {
			// 	panic(err)
			// }
		}
	}
	hcd, ok := vcd.(h264parser.CodecData)
	if !ok {
		panic(stop)
	}
	dec, err := NewH264Decoder(hcd.Record)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Video idx %d audio idx %d\n", videoidx, audioidx)
	i := 0
	for {
		pkt, err := file.ReadPacket()
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Println("Error reading packet")
			panic(err)
		}
		fmt.Printf("Packet idx %d key %v time %s\n", pkt.Idx, pkt.IsKeyFrame, pkt.Time)
		if pkt.Idx == int8(videoidx) {
			// Packet2Image(vcd, pkt)
			img, err := dec.Decode(pkt.Data)
			if err != nil {
				// panic(err)
				fmt.Println(err)
			} else {
				// fmt.Println(img)
				f, err := os.Create(fmt.Sprintf("img_%d.jpg", i))
				if err != nil {
					panic(err)
				}
				jpeg.Encode(f, img, nil)
				f.Close()
				i++
			}
			// frame, err := vdec.Decode(pkt.Data)
			// if err != nil {
			// 	panic(err)
			// }
			// fmt.Println("decode samples", frame.Image)
		}
	}

}
*/
