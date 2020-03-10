package testers

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"sort"
	"strconv"
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
	"github.com/patrickmn/go-cache"
	"golang.org/x/text/message"
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

var (
	// ErrZeroStreams ...
	ErrZeroStreams = errors.New("Zero streams")
	// ErrStreamOpenFailed ...
	ErrStreamOpenFailed = errors.New("Stream open failed")

	mp          = message.NewPrinter(message.MatchLanguage("en"))
	mhttpClient = &http.Client{
		// Transport: &http2.Transport{TLSClientConfig: tlsConfig},
		// Transport: &http2.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: false}},
		// Transport: &http2.Transport{AllowHTTP: true},
		Timeout: 4 * time.Second,
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
	started := time.Now()
	var streamsNoSegmentsAnymore []string
	failedStreams := cache.New(5*time.Minute, 8*time.Minute)
	err := mc.startStreams(failedStreams)
	if err != nil {
		return err
	}
	emsg := fmt.Sprintf("Started **%d** Picarto streams", len(mc.downloaders))
	messenger.SendMessage(emsg)

	time.Sleep(120 * time.Second)
	// time.Sleep(12 * time.Second)
	for {
		activeStreams, err := mc.activeStreams()
		if err != nil {
			glog.Errorf("Error getting active streams err=%v", err)
			time.Sleep(5 * time.Second)
			continue
		}
		if len(activeStreams) < mc.streamsNum || len(streamsNoSegmentsAnymore) > 0 {
			// remove old downloaders
			for sn, mt := range mc.downloaders {
				noSegs := utils.StringsSliceContains(streamsNoSegmentsAnymore, sn)
				notActive := !utils.StringsSliceContains(activeStreams, sn)
				if notActive || noSegs {
					mt.Stop()
					reason := "not in active streams anymore"
					if noSegs {
						reason = "no segments downloaded for 30s"
					}
					messenger.SendMessage(fmt.Sprintf("Stopped stream **%s** because %s", sn, reason))
					delete(mc.downloaders, sn)
					failedStreams.SetDefault(sn, true)
				}
			}
			// need to start new streams
			mc.startStreams(failedStreams)
		}
		var downSource, downTrans int
		var ds2all downStats2
		now := time.Now()
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
			ds2 := mt.getDownStats2()
			if !ds2.lastDownloadTime.IsZero() && now.Sub(ds2.lastDownloadTime) > 30*time.Second {
				streamsNoSegmentsAnymore = append(streamsNoSegmentsAnymore, sn)
			}
			ds2all.add(ds2)
			emsg := fmt.Sprintf("Stream __%s__ success rate: **%f**%% (%d/%d) (num proflies %d)", sn,
				ds2.successRate, ds2.downTransAll, ds2.downSource, ds2.numProfiles)
			messenger.SendMessage(emsg)
			time.Sleep(10 * time.Millisecond)
			if picartoDebug {
				for _, mdkey := range mt.downloadsKeys {
					md := mt.downloads[mdkey]
					md.mu.Lock()
					glog.Infof("=========> down segments for %s %s len=%d", md.name, md.resolution, len(md.downloadedSegments))
					ps := picartoSortedSegments(md.downloadedSegments)
					sort.Sort(ps)
					glog.Infof("\n%s", strings.Join(ps, "\n"))
					md.mu.Unlock()
				}
			}
		}
		runningFor := time.Since(started)
		emsg := mp.Sprintf("Number of streams: **%d** success rate: **%7.4f**%% (%d/%d) bytes downloaded %d/%d (transcoded is%f%% of source bandwitdh) running for %s", len(mc.downloaders),
			ds2all.successRate, ds2all.downTransAll, ds2all.downSource, ds2all.transAllBytes, ds2all.sourceBytes, float64(ds2all.transAllBytes)/float64(ds2all.sourceBytes)*100, runningFor)
		messenger.SendMessage(emsg)
		/*
			if downSource > 0 {
				emsg := fmt.Sprintf("Number of streams: **%d** success rate: **%f** (%d/%d)", len(mc.downloaders),
					float64(downTrans)/float64(downSource)*100.0, downTrans, downSource)
				glog.Infoln(emsg)
				messenger.SendMessage(emsg)
			}
		*/
		if model.Production {
			time.Sleep(120 * time.Second)
		} else {
			time.Sleep(32 * time.Second)
		}
	}
}

func (mc *MistController) startStreams(failedStreams *cache.Cache) error {
	ps, err := picarto.GetOnlineUsers(picartoCountry, mc.adult, mc.gaming)
	if err != nil {
		return err
	}
	// start initial downloaders
	var started []string
	var uri string
	var shouldSkip [][]string
streamsLoop:
	for i := 0; len(mc.downloaders) < mc.streamsNum && i < len(ps); i++ {
		userName := ps[i].Name
		// userName = "Felino"
		if utils.StringsSliceContains(started, userName) {
			continue
		}
		if _, has := failedStreams.Get(userName); has {
			continue
		}
		if _, has := mc.downloaders[userName]; has {
			continue
		}
		for try := 0; try < 3; try++ {
			uri, shouldSkip, err = mc.startStream(userName)
			if err == nil {
				break
			}
			messenger.SendMessage(fmt.Sprintf("Error starting Picarto stream pull user=%s err=%v started so far %d try %d",
				userName, err, len(mc.downloaders), try))
			if err == ErrStreamOpenFailed || timedout(err) || err == io.EOF {
				failedStreams.SetDefault(userName, true)
				continue streamsLoop
			}
			time.Sleep((200*time.Duration(try) + 300) * time.Millisecond)
		}
		if err != nil {
			failedStreams.SetDefault(userName, true)
			continue
		}

		mt := newM3UTester(mc.ctx, mc.ctx.Done(), nil, false, true, true, false, false, nil, shouldSkip)
		mc.downloaders[userName] = mt
		mt.Start(uri)
		messenger.SendMessage(fmt.Sprintf("Started stream %s", uri))
		started = append(started, userName)
		time.Sleep(50 * time.Millisecond)
	}
	if len(started) == 0 {
		err = fmt.Errorf("Wasn't able to start any stream on Picarto")
		return err
	}
	return nil
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

func (mc *MistController) startStream(userName string) (string, [][]string, error) {
	uri := fmt.Sprintf(hlsURLTemplate, mc.mistHot, userName)
	glog.Infof("Starting to pull from user=%s uri=%s", userName, uri)
	var try int
	var err error
	var mediaURIs []string
	for {
		mediaURIs, err = mc.pullFirstTime(uri)
		if err != nil {
			if err == ErrZeroStreams || err == ErrStreamOpenFailed || timedout(err) || err == io.EOF {
				return "", nil, err
			}
		}
		if len(mediaURIs) >= mc.profilesNum+1 {
			break
		}
		if try > 7 {
			return "", nil, fmt.Errorf("Stream uri=%s did not started transcoding lasterr=%v", uri, err)
		}
		try++
		time.Sleep((time.Duration(try) + 1) * 500 * time.Millisecond)
	}
	mpullres := make([]*plPullRes, len(mediaURIs))
	mediaPulCh := make(chan *plPullRes, len(mediaURIs))
	for i, muri := range mediaURIs {
		go mc.pullMediaPL(muri, i, mediaPulCh)
	}
	for i := 0; i < len(mediaURIs); i++ {
		res := <-mediaPulCh
		mpullres[res.i] = res
	}
	for i := 0; i < len(mediaURIs); i++ {
		if mpullres[i].err != nil {
			return uri, nil, mpullres[i].err
		}
	}
	// find first transcoded segment time
	transTime := mistGetTimeFromSegURI(mpullres[1].pl.Segments[0].URI)
	shouldSkip := make([][]string, len(mediaURIs))
	found := false
	for si, seg := range mpullres[0].pl.Segments {
		if seg == nil {
			// not found
			break
		}
		sourceTime := mistGetTimeFromSegURI(seg.URI)
		glog.Infof("Trans time %d source time %d i %d", transTime, sourceTime, si)
		if absDiff(transTime, sourceTime) < 200 {
			found = true
			for i := 0; i < si; i++ {
				shouldSkip[0] = append(shouldSkip[0], mistSessionRE.ReplaceAllString(mpullres[0].pl.Segments[i].URI, ""))
			}
			glog.Infof("Found! shoud skip %d source segments (%+v)", si, shouldSkip[0])
			break
		}
	}
	if !found {
		// not found, do reverse
		sourceTime := mistGetTimeFromSegURI(mpullres[0].pl.Segments[0].URI)
		for si, seg := range mpullres[1].pl.Segments {
			if seg == nil {
				// not found
				break
			}
			transTime := mistGetTimeFromSegURI(seg.URI)
			glog.Infof("Source time %d trans time %d i %d", sourceTime, transTime, si)
			if absDiff(transTime, sourceTime) < 200 {
				for i := 0; i < si; i++ {
					shouldSkip[1] = append(shouldSkip[1], mistSessionRE.ReplaceAllString(mpullres[1].pl.Segments[i].URI, ""))
				}
				glog.Infof("Found! shoud skip %d transcoded segments (%+v)", si, shouldSkip[1])
				break
			}
		}
	}
	return uri, shouldSkip, nil
}

func absDiff(i1, i2 int) int {
	r := i1 - i2
	if r < 0 {
		return -r
	}
	return r
}

// takes segment's URI like this `13223899_13225899.ts?sessId=20275` and return 13223899
func mistGetTimeFromSegURI(segURI string) int {
	up := strings.Split(segURI, "_")
	pts, _ := strconv.Atoi(up[0])
	return pts
}

func timedout(e error) bool {
	t, ok := e.(interface {
		Timeout() bool
	})
	return ok && t.Timeout()
}

func (mc *MistController) pullFirstTime(uri string) ([]string, error) {
	resp, err := mhttpClient.Do(uhttp.GetRequest(uri))
	if err != nil {
		to := timedout(err)
		glog.Infof("===== get error (timed out: %v) getting master playlist %s: %v ", to, uri, err)
		return nil, err
	}
	b, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		glog.Infof("===== error getting master playlist body uri=%s err=%v", uri, err)
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		err := fmt.Errorf("===== status error getting master playlist %s: %v (%s) body: %s", uri, resp.StatusCode, resp.Status, string(b))
		return nil, err
	}
	if strings.Contains(string(b), "Stream open failed") {
		glog.Errorf("Master playlist stream open failed uri=%s", uri)
		return nil, ErrStreamOpenFailed
	}
	mpl := m3u8.NewMasterPlaylist()
	// err = mpl.DecodeFrom(resp.Body, true)
	err = mpl.Decode(*bytes.NewBuffer(b), true)
	// resp.Body.Close()
	if err != nil {
		glog.Infof("===== error getting master playlist uri=%s err=%v", uri, err)
		return nil, err
	}
	glog.V(model.VVERBOSE).Infof("Got master playlist with %d variants (%s):", len(mpl.Variants), uri)
	glog.V(model.VVERBOSE).Info(mpl)
	// glog.Infof("Got master playlist with %d variants (%s):", len(mpl.Variants), uri)
	// glog.Info(mpl)
	if len(mpl.Variants) < 1 {
		glog.Infof("Playlist for uri=%s has streams=%d", uri, len(mpl.Variants))
		return nil, ErrZeroStreams
	}
	masterURI, _ := url.Parse(uri)
	r := make([]string, 0, len(mpl.Variants))
	for _, va := range mpl.Variants {
		pvrui, err := url.Parse(va.URI)
		if err != nil {
			glog.Error(err)
			return nil, err
		}
		// glog.Infof("Parsed uri: %+v", pvrui, pvrui.IsAbs)
		if !pvrui.IsAbs() {
			pvrui = masterURI.ResolveReference(pvrui)
		}
		// glog.Info(pvrui)
		r = append(r, pvrui.String())
	}
	return r, nil
}

type plPullRes struct {
	pl  *m3u8.MediaPlaylist
	err error
	i   int
}

func (mc *MistController) pullMediaPL(uri string, i int, out chan *plPullRes) (*m3u8.MediaPlaylist, error) {
	resp, err := mhttpClient.Do(uhttp.GetRequest(uri))
	if err != nil {
		glog.Infof("===== get error getting media playlist %s: %v", uri, err)
		out <- &plPullRes{err: err, i: i}
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		err := fmt.Errorf("===== status error getting media playlist %s: %v (%s) body: %s", uri, resp.StatusCode, resp.Status, string(b))
		out <- &plPullRes{err: err, i: i}
		return nil, err
	}
	mpl, _ := m3u8.NewMediaPlaylist(100, 100)
	err = mpl.DecodeFrom(resp.Body, true)
	// err = mpl.Decode(*bytes.NewBuffer(b), true)
	resp.Body.Close()
	if err != nil {
		glog.Infof("===== error getting media playlist uri=%s err=%v", uri, err)
		out <- &plPullRes{err: err, i: i}
		return nil, err
	}
	cs := countSegments(mpl)
	glog.V(model.VVERBOSE).Infof("Got media playlist with count=%d len=%d mc=%d segmens (%s):", mpl.Count(), mpl.Len(), cs, uri)
	glog.V(model.VVERBOSE).Info(mpl)
	// glog.Infof("Got media playlist with count=%d len=%d mc=%d segmens (%s):", mpl.Count(), mpl.Len(), cs, uri)
	// glog.Info(mpl)
	if cs < 1 {
		glog.Infof("Playlist for uri=%s has zero segments", uri)
		out <- &plPullRes{err: ErrZeroStreams, i: i}
		panic("no segments")
		return nil, ErrZeroStreams
	}
	out <- &plPullRes{pl: mpl, i: i}
	return mpl, nil
}

type picartoSortedSegments []string

func (p picartoSortedSegments) Len() int { return len(p) }
func (p picartoSortedSegments) Less(i, j int) bool {
	return mistGetTimeFromSegURI(p[i]) < mistGetTimeFromSegURI(p[j])
}
func (p picartoSortedSegments) Swap(i, j int) { p[i], p[j] = p[j], p[i] }
