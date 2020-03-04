package testers

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/m3u8"
	"github.com/livepeer/stream-tester/apis/mist"
	"github.com/livepeer/stream-tester/apis/picarto"
	"github.com/livepeer/stream-tester/internal/utils"
	"github.com/livepeer/stream-tester/internal/utils/uhttp"
	"github.com/livepeer/stream-tester/messenger"
	"github.com/livepeer/stream-tester/model"
)

const (
	picartoCountry = "us-east1"
	hlsURLTemplate = "http://%s:8080/hls/golive+%s/index.m3u8"
	baseStreamName = "golive"
)

type (
	// MistController pulls Picarto streams into Mist server, calculates success rate of transcoding
	// makes sure that number of streams is constant (if source Picarto stream disconnects, then adds
	// new stream)
	MistController struct {
		mapi        *mist.API
		mistHot     string
		profilesNum int // transcoding profiles number. Should be one for now.
		adult       bool
		gaming      bool
		streamsNum  int                   // number of streams to maintain
		downloaders map[string]*m3utester // [Picarto name]
		ctx         context.Context
		cancel      context.CancelFunc
	}
)

// NewMistController creates new MistController
func NewMistController(mistHost string, streamsNum, profilesNum int, adult, gaming bool, mapi *mist.API) *MistController {
	ctx, cancel := context.WithCancel(context.Background())
	return &MistController{
		mapi:        mapi,
		mistHot:     mistHost,
		adult:       adult,
		gaming:      gaming,
		streamsNum:  streamsNum,
		profilesNum: profilesNum,
		downloaders: make(map[string]*m3utester),
		ctx:         ctx,
		cancel:      cancel,
	}
}

// Start blocks forever. Returns error if can't start
func (mc *MistController) Start() error {
	err := mc.mainLoop()
	return err
}

func (mc *MistController) mainLoop() error {
	ps, err := picarto.GetOnlineUsers(picartoCountry, mc.adult, mc.gaming)
	if err != nil {
		return err
	}
	// start initial downloaders
	for i := 0; i < mc.streamsNum && i < len(ps); i++ {
		userName := ps[i].Name
		uri := fmt.Sprintf(hlsURLTemplate, mc.mistHot, userName)
		glog.Infof("Starting to pull from user=%s uri=%s", userName, uri)
		var try int
		for {
			vn, err := mc.pullFirstTime(uri)
			if err != nil {
				return err
			}
			if vn >= mc.profilesNum+1 {
				break
			}
			if try > 5 {
				return fmt.Errorf("Stream uri=%s did not started transcoding", uri)
			}
			try++
			time.Sleep((time.Duration(try) + 1) * 500 * time.Millisecond)
		}
		mt := newM3UTester(mc.ctx.Done(), nil, false, true, false, false, nil, mc.ctx)
		mc.downloaders[userName] = mt
		mt.Start(uri)
		messenger.SendMessage(uri)
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(30 * time.Second)
	for {
		activeStreams, err := mc.activeStreams()
		if err != nil {
			glog.Errorf("Error getting active streams err=%v", err)
			time.Sleep(5 * time.Second)
			continue
		}
		if len(activeStreams) < mc.streamsNum {
			// remove old downloaders
			for sn, mt := range mc.downloaders {
				if !utils.StringsSliceContains(activeStreams, sn) {
					mt.Stop()
					glog.Infof("Stopped stream name=%s because not in active streams anymore", sn)
					delete(mc.downloaders, sn)
				}
			}
			// need to start new streams
		}
		var downSource, downTrans int
		for sn, mt := range mc.downloaders {
			stats := mt.statsSeparate()
			glog.Infoln(strings.Repeat("*", 100))
			glog.Infof("====> download stats for %s:", sn)
			for _, st := range stats {
				glog.Infof("====> resolution=%s source=%v", st.resolution, st.source)
				glog.Infof("%+v", st)
				if st.source {
					downSource += st.success
				} else {
					downTrans += st.success
				}
			}
		}
		if downSource > 0 {
			emsg := fmt.Sprintf("Number of streams: **%d** success rate: **%f**", len(mc.downloaders), float64(downTrans)/float64(downSource)*100.0)
			glog.Infoln(emsg)
			messenger.SendMessage(emsg)
		}

		time.Sleep(120 * time.Second)
	}
}

func (mc *MistController) activeStreams() ([]string, error) {
	_, activeStreams, err := mc.mapi.Streams()
	if err != nil {
		return nil, err
	}
	var asr []string
	for _, as := range activeStreams {
		if strings.HasPrefix(as, baseStreamName+"+") {
			asp := strings.Split(as, "+")
			if len(asp) < 2 {
				continue
			}
			asr = append(asr, asp[1])
		}
	}
	return asr, nil
}

func (mc *MistController) pullFirstTime(uri string) (int, error) {
	resp, err := httpClient.Do(uhttp.GetRequest(uri))
	if err != nil {
		glog.Infof("===== get error getting master playlist %s: %v", uri, err)
		return 0, err
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		err := fmt.Errorf("===== status error getting master playlist %s: %v (%s) body: %s", uri, resp.StatusCode, resp.Status, string(b))
		return 0, err
	}
	// b, err := ioutil.ReadAll(resp.Body)
	// if err
	mpl := m3u8.NewMasterPlaylist()
	err = mpl.DecodeFrom(resp.Body, true)
	// err = mpl.Decode(*bytes.NewBuffer(b), true)
	resp.Body.Close()
	if err != nil {
		glog.Infof("===== error getting master playlist uri=%s err=%v", uri, err)
		return 0, err
	}
	glog.V(model.VVERBOSE).Infof("Got master playlist with %d variants (%s):", len(mpl.Variants), uri)
	glog.V(model.VVERBOSE).Info(mpl)
	glog.Infof("Got master playlist with %d variants (%s):", len(mpl.Variants), uri)
	glog.Info(mpl)
	// glog.Infof("Got master playlist with %d variants (%s):", len(mpl.Variants), surl)
	if len(mpl.Variants) < 1 {
		return 0, fmt.Errorf("Playlist for uri=%s has streams=%d", uri, len(mpl.Variants))
	}
	return len(mpl.Variants), nil
}
