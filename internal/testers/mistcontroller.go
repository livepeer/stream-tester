package testers

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Necroforger/dgrouter/exrouter"
	"github.com/golang/glog"
	"github.com/livepeer/catalyst-api/mapic/apis/mist"
	"github.com/livepeer/m3u8"
	"github.com/livepeer/stream-tester/apis/picarto"
	"github.com/livepeer/stream-tester/internal/utils"
	"github.com/livepeer/stream-tester/internal/utils/uhttp"
	"github.com/livepeer/stream-tester/messenger"
	"github.com/livepeer/stream-tester/model"
	"github.com/patrickmn/go-cache"
)

const (
	// picartoCountry       = "us-east1"
	hlsURLTemplate       = "http://%s:8080/hls/golive+%s/index.m3u8"
	baseStreamName       = "golive"
	mainLoopStepDuration = 32 * time.Second
)

type (
	// MistController pulls Picarto streams into Mist server, calculates success rate of transcoding
	// makes sure that number of streams is constant (if source Picarto stream disconnects, then adds
	// new stream)
	MistController struct {
		mapi               *mist.API
		picartoCountry     string
		mistHot            string
		profilesNum        int // transcoding profiles number. Should be one for now.
		adult              bool
		gaming             bool
		save               bool
		streamsNum         int     // number of streams to maintain
		sdCutOff           float64 // do not start streams that have standard deviation of segments durations more than that
		externalHost       string
		blackListedStreams []string
		statsInterval      time.Duration
		downloaders        map[string]*m3utester // [Picarto name]
		ctx                context.Context
		cancel             context.CancelFunc
		startSignal        chan bool
		stopSignal         chan bool
		started            bool
	}
)

var (
	// ErrZeroStreams ...
	ErrZeroStreams = errors.New("Zero streams")
	// ErrNoAudioInStream ...
	ErrNoAudioInStream = errors.New("No audio in stream")
	// ErrBigTimeDifference ...
	ErrBigTimeDifference = errors.New("Time difference too big")
	// ErrNoMatchingSegments ...
	ErrNoMatchingSegments = errors.New("Can't match transcoded segments to source segments")
	// ErrTooBigDurationsDeviation ...
	ErrTooBigDurationsDeviation = errors.New("Too big deviation of segment's durations")

	mhttpClient = &http.Client{
		// Transport: &http2.Transport{TLSClientConfig: tlsConfig},
		// Transport: &http2.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: false}},
		// Transport: &http2.Transport{AllowHTTP: true},
		Timeout: 4 * time.Second,
	}
)

// NewMistController creates new MistController
func NewMistController(mistHost string, streamsNum, profilesNum int, adult, gaming, save bool, mapi *mist.API, blackListedStreams, externalHost string,
	statsInterval time.Duration, sdCutOff float64, country string) *MistController {

	statsDelay := 4 * 60 * time.Second
	if !model.Production {
		statsDelay = 32 * time.Second
		// statsDelay = 3 * time.Second
	}
	if statsInterval != 0 {
		statsDelay = statsInterval
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &MistController{
		picartoCountry:     country,
		mapi:               mapi,
		mistHot:            mistHost,
		adult:              adult,
		gaming:             gaming,
		save:               save,
		streamsNum:         streamsNum,
		externalHost:       externalHost,
		profilesNum:        profilesNum,
		statsInterval:      statsDelay,
		sdCutOff:           sdCutOff,
		blackListedStreams: strings.Split(blackListedStreams, ","),
		downloaders:        make(map[string]*m3utester),
		ctx:                ctx,
		cancel:             cancel,
		startSignal:        make(chan bool),
		stopSignal:         make(chan bool),
	}
}

// Start blocks forever. Returns error if can't start
func (mc *MistController) Start() error {
	bc := messenger.AddBotCommand("picarto", mc.botCommand)
	if bc != nil {
		bc.Desc("num gets/sets number of streams")
	}
	ec := make(chan error)
	go mc.mainLoop(ec)
	err := <-ec
	return err
}

func (mc *MistController) mainLoop(ec chan error) error {
	go mc.workLoop(ec, true)
	mc.started = true
	for {
		select {
		case <-mc.stopSignal:
			if mc.started {
				mc.cancel()
				mc.started = false
			}
		case <-mc.startSignal:
			if !mc.started {
				ctx, cancel := context.WithCancel(context.Background())
				mc.ctx = ctx
				mc.cancel = cancel
				go mc.workLoop(ec, false)
				mc.started = true
			}
		}
	}
}

func (mc *MistController) workLoop(ec chan error, firstTime bool) error {
	emsg := fmt.Sprintf("Starting **%d** Picarto streams (ver %s)", mc.streamsNum, model.Version)
	messenger.SendMessage(emsg)
	started := time.Now()
	var streamsNoSegmentsAnymore []string
	failedStreams := cache.New(5*time.Minute, 8*time.Minute)
	starting := make(map[string]bool)
	/*
			toBeStarted := mc.streamsNum
			if toBeStarted > streamsStartStep {
				toBeStarted = streamsStartStep
			}
			err := mc.startStreams(failedStreams, toBeStarted)
			if err != nil {
				return err
			}
			time.Sleep(500 * time.Millisecond)
			emsg := fmt.Sprintf("Started **%d** Picarto streams\n", len(mc.downloaders))
			sms := make([]string, 0, len(mc.downloaders))
			for _, d := range mc.downloaders {
				sms = append(sms, d.initialURL.String())
			}
			emsg += strings.Join(sms, "\n")
			messenger.SendMessage(emsg)

		time.Sleep(mainLoopStepDuration)
	*/
	// var lastTimeStatsShown time.Time
	activityCheck := time.NewTicker(100 * time.Millisecond)
	statsCheck := time.NewTicker(mc.statsInterval)
	// firstTime := true
	sRes := make(chan *startRes, 32)
	for {
		select {
		case <-mc.ctx.Done():
			glog.Infof("====>>> returning from worker loop")
			return nil
		case sr := <-sRes:
			if !sr.finished {
				if sr.err != nil {
					messenger.SendMessage(fmt.Sprintf("Error starting Picarto stream pull user=%s err=%v started so far %d staring %d try %d",
						sr.name, sr.err, len(mc.downloaders), len(starting), sr.try))
				}
			} else {
				delete(starting, sr.name)
				if sr.err != nil {
					messenger.SendMessage(fmt.Sprintf("Fatal error starting Picarto stream pull user=%s err=%v started so far %d try %d",
						sr.name, sr.err, len(mc.downloaders), sr.try))
					failedStreams.SetDefault(sr.name, true)
					// if isFatalError(err) {
					// 	continue streamsLoop
					// }
				} else {
					mc.downloaders[sr.name] = sr.mt
				}
			}
		case <-activityCheck.C:
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
					notActive = false
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
			}
			if len(mc.downloaders)+len(starting) < mc.streamsNum {
				/*
					toBeStarted := mc.streamsNum
					if toBeStarted-len(mc.downloaders) > streamsStartStep {
						toBeStarted = len(mc.downloaders) + streamsStartStep
					}
					// need to start new streams
					mc.startStreams(failedStreams, toBeStarted)
				*/
				// toBeStarted := mc.streamsNum - len(mc.downloaders) + len(starting)
				ps, err := picarto.GetOnlineUsers(mc.picartoCountry, mc.adult, mc.gaming)
				if err != nil {
					if firstTime {
						ec <- err
						return err
					}
					continue
				}
				if firstTime {
					activityCheck = time.NewTicker(mainLoopStepDuration)
					firstTime = false
				}
				for i := 0; len(mc.downloaders)+len(starting) < mc.streamsNum && i < len(ps); i++ {
					userName := ps[i].Name
					// userName = "Noerbmu"
					// userName = "NylaTheWolf"
					// userName = "rudeezy"
					// userName = "snooze"
					// userName = "FreeMusicLive"
					// userName = "Amodwithoutamark"
					// userName = "ajfjsgo"
					// userName = "StudioNLM"
					// userName = "Nasnency"
					// userName = "PurpleLemons"
					// userName = "MEDYUM"
					// userName = "RTI"
					// userName = "balanceduniverse"
					// userName = "BlindCoyote"
					// userName = "AnnMaulina"
					// userName = "KepArtem"
					// userName = "Artzipants"
					// userName = "MiiNiiRU"
					// userName = "drawdle"
					// userName = "unwulfd"
					// userName = "Mangopoptart"
					// userName = "SNOWDON"
					// userName = "hasnogame"
					// userName = "Aciel"
					// userName = "HoneyFoxArt"
					// userName = "Ana"
					// userName = "FearDaKez"
					if _, has := failedStreams.Get(userName); has {
						continue
					}
					if _, has := mc.downloaders[userName]; has {
						continue
					}
					if _, has := starting[userName]; has {
						continue
					}
					starting[userName] = true
					go mc.startOneStream(userName, sRes)
				}
			}
		case <-statsCheck.C:
			var downSource, downTrans int
			var ds2all downStats2
			now := time.Now()
			// ssm := make([]string, 0, len(mc.downloaders))
			var ssm []*messenger.DiscordEmbed
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
				// emsg := fmt.Sprintf("Stream __%s__ success rate: **%f%%** (%d/%d) (num proflies %d)", sn,
				// 	ds2.successRate, ds2.downTransAll, ds2.downSource, ds2.numProfiles)
				// ssm = append(ssm, emsg)

				/*
					emmsg := messenger.NewDiscordEmbed(fmt.Sprintf("Stream __%s__", sn))
					emmsg.URL = mt.initialURL.String()
					emmsg.Color = successRate2Color(ds2.successRate)
					emmsg.AddFieldF("Success rate", true, "**%f%%**", ds2.successRate)
					emmsg.AddFieldF("Segments trans/source", true, "%d/%d", ds2.downTransAll, ds2.downSource)
					emmsg.AddFieldF("Num proflies", true, "%d", ds2.numProfiles)
				*/
				// if ds2.successRate < 100 {
				ssm = append(ssm, ds2.discordRichMesage(fmt.Sprintf("Stream __%s__", sn), mc.externalHost, true))
				// }
				// messenger.SendRichMessage(emmsg)
				// messenger.SendMessage(emsg)
				// time.Sleep(10 * time.Millisecond)
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
			// messenger.SendMessageSlice(ssm)
			messenger.SendRichMessage(ssm...)
			runningFor := time.Since(started)
			// emsg := mp.Sprintf("Number of streams: **%d** success rate: **%7.4f%%** (%d/%d) bytes downloaded %d/%d (transcoded is **%4.2f%%** of source bandwitdh) running for %s", len(mc.downloaders),
			// 	ds2all.successRate, ds2all.downTransAll, ds2all.downSource, ds2all.transAllBytes, ds2all.sourceBytes, float64(ds2all.transAllBytes)/float64(ds2all.sourceBytes)*100, runningFor)
			// messenger.SendMessage(emsg)

			emmsg := ds2all.discordRichMesage(fmt.Sprintf("Number of streams **%d**", len(mc.downloaders)), mc.externalHost, false)
			emmsg.URL = ""
			/*
				emmsg := messenger.NewDiscordEmbed(fmt.Sprintf("Number of streams **%d**", len(mc.downloaders)))
				emmsg.Color = successRate2Color(ds2all.successRate)
				emmsg.AddFieldF("Success rate", true, "**%7.4f%%**", ds2all.successRate)
				emmsg.AddFieldF("Bytes downloaded", true, "%d/%d", ds2all.transAllBytes, ds2all.sourceBytes)
				var pob float64
				if ds2all.sourceBytes > 0 {
					pob = float64(ds2all.transAllBytes) / float64(ds2all.sourceBytes) * 100
				}
				emmsg.AddFieldF("Percent of source bandwitdh", true, "**%4.2f%%**", pob)
				emmsg.AddFieldF("Segments trans/source", true, "%d/%d", ds2all.downTransAll, ds2all.downSource)
			*/
			emmsg.AddFieldF("Running for", true, "%s", runningFor)
			messenger.SendRichMessage(emmsg)
			/*
				if downSource > 0 {
					emsg := fmt.Sprintf("Number of streams: **%d** success rate: **%f** (%d/%d)", len(mc.downloaders),
						float64(downTrans)/float64(downSource)*100.0, downTrans, downSource)
					glog.Infoln(emsg)
					messenger.SendMessage(emsg)
				}
			*/
			// lastTimeStatsShown = time.Now()
			// time.Sleep(mainLoopStepDuration)
		}
	}
}

func (mc *MistController) botCommand(ctx *exrouter.Context) {
	// ctx.Reply("picarto called with arguments:\n", strings.Join(ctx.Args, ";"))
	if len(ctx.Args) == 1 {
		ctx.Reply(fmt.Sprintf("Pulling %d streams", mc.streamsNum))
		return
	}
	switch ctx.Args[1] {
	case "help":
		ctx.Reply("**num** [value] gets/sets number of stream\n**stop** stops pulling\n**start** starts pulling")
	case "stop":
		mc.stopSignal <- true
		ctx.Reply("Stopped pulling streams")
	case "start":
		ctx.Reply("Starting pulling streams")
		mc.startSignal <- true
	case "num":
		if len(ctx.Args) == 2 {
			ctx.Reply(fmt.Sprintf("Pulling %d streams", mc.streamsNum))
		} else if len(ctx.Args) > 2 {
			nn, _ := strconv.Atoi(ctx.Args[2])
			if nn > 0 {
				mc.streamsNum = nn
				ctx.Reply(fmt.Sprintf("Number of streams set to %d", mc.streamsNum))
			}
		}
	}
}

type startRes struct {
	name     string
	finished bool
	err      error
	try      int
	mt       *m3utester
	// isFatal  bool
}

func (mc *MistController) startOneStream(streamName string, resp chan *startRes) {
	var err, mediaErr error
	var uri string
	var shouldSkip [][]string
	for try := 0; try < 12; try++ {
		select {
		case <-mc.ctx.Done():
			return
		default:
		}
		uri, shouldSkip, err, mediaErr = mc.startStream(streamName)
		if err == nil && mediaErr == nil {
			break
		}
		isFatal := isFatalError(err, try)
		if err == nil && mediaErr != nil {
			err = mediaErr
		}
		resp <- &startRes{name: streamName, err: err, try: try, finished: isFatal}
		if isFatal {
			return
		}
		/*
			messenger.SendMessage(fmt.Sprintf("Error starting Picarto stream pull user=%s err=%v started so far %d try %d",
				userName, err, len(mc.downloaders), try))
			if isFatalError(err) {
				failedStreams.SetDefault(userName, true)
				continue streamsLoop
			}
		*/
		time.Sleep((200*time.Duration(try) + 300) * time.Millisecond)
	}
	if err != nil || mediaErr != nil {
		// failedStreams.SetDefault(userName, true)
		if err == nil {
			err = mediaErr
		}
		resp <- &startRes{name: streamName, err: err, finished: true}
		return
	}

	mt := newM3UTester(mc.ctx, nil, false, true, true, false, mc.save, nil, shouldSkip, streamName)
	// mc.downloaders[userName] = mt
	mt.Start(uri)
	messenger.SendMessage(fmt.Sprintf("Started stream %s", mc.makeExternalURL(uri)))
	resp <- &startRes{name: streamName, mt: mt, finished: true}
}

func (mc *MistController) makeExternalURL(iurl string) string {
	if mc.externalHost == "" || iurl == "" {
		return iurl
	}
	pu, _ := url.Parse(iurl)
	port := pu.Port()
	pu.Host = mc.externalHost + ":" + port
	return pu.String()
}

/*
func (mc *MistController) startStreams(failedStreams *cache.Cache, streamsNum int) error {
	ps, err := picarto.GetOnlineUsers(picartoCountry, mc.adult, mc.gaming)
	if err != nil {
		return err
	}
	ps = ps[10:]
	// start initial downloaders
	var started []string
	var uri string
	var shouldSkip [][]string
streamsLoop:
	for i := 0; len(mc.downloaders) < streamsNum && i < len(ps); i++ {
		userName := ps[i].Name
		// userName = "Felino"
		// userName = "AwfulRabbit"
		// userName = "axonradiolive"
		// userName = "playloudlive"
		// userName = "forbae"
		// userName = "Noerbmu"
		// userName = "NylaTheWolf"
		if utils.StringsSliceContains(started, userName) || utils.StringsSliceContains(mc.blackListedStreams, userName) {
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
			if isFatalError(err) {
				failedStreams.SetDefault(userName, true)
				continue streamsLoop
			}
			time.Sleep((200*time.Duration(try) + 300) * time.Millisecond)
		}
		if err != nil {
			failedStreams.SetDefault(userName, true)
			continue
		}

		mt := newM3UTester(mc.ctx, mc.ctx.Done(), nil, false, true, true, false, mc.save, nil, shouldSkip, userName)
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
*/

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

func (mc *MistController) startStream(userName string) (string, [][]string, error, error) {
	uri := fmt.Sprintf(hlsURLTemplate, mc.mistHot, userName)
	glog.Infof("Starting to pull from user=%s uri=%s", userName, uri)
	var try int
	var err error
	var mediaURIs []string
	for {
		select {
		case <-mc.ctx.Done():
			return "", nil, fmt.Errorf("stopped"), nil
		default:
		}
		mediaURIs, err = mc.pullFirstTime(uri)
		if err != nil {
			if isFatalError(err, try) {
				return "", nil, err, nil
			}
		}
		if len(mediaURIs) >= mc.profilesNum+1 {
			break
		}
		if try > 7 {
			return "", nil, fmt.Errorf("Stream uri=%s did not started transcoding lasterr=%w", uri, err), nil
		}
		try++
		time.Sleep((time.Duration(try) + 1) * 500 * time.Millisecond)
	}
	mpullres := make([]*plPullRes, len(mediaURIs))
	mediaPulCh := make(chan *plPullRes, len(mediaURIs))
	for i, muri := range mediaURIs {
		go mc.pullMediaPL(userName, muri, i, mediaPulCh)
	}
	for i := 0; i < len(mediaURIs); i++ {
		res := <-mediaPulCh
		mpullres[res.i] = res
	}
	for i := 0; i < len(mediaURIs); i++ {
		if mpullres[i].err != nil {
			return uri, nil, nil, mpullres[i].err
		}
		if mc.sdCutOff > 0.0 && mpullres[i].standardDeviation > mc.sdCutOff {
			return uri, nil, fmt.Errorf("%w: standard deviation is %f, threshold is %f", ErrTooBigDurationsDeviation, mpullres[i].standardDeviation, mc.sdCutOff), nil
		}
	}
	if mpullres[0].firstSegmentParseError != nil {
		return uri, nil, mpullres[0].firstSegmentParseError, nil
	}
	// check difference between timestamps in source and transcoded streams
	sourceTime := mistGetTimeFromSegURI(mpullres[0].pl.Segments[0].URI)
	transTime := mistGetTimeFromSegURI(mpullres[1].pl.Segments[0].URI)
	if absDiff(sourceTime, transTime) > 10*60*1000 { // 10 min
		// panic(fmt.Errorf("Diffeerence is %d", absDiff(sourceTime, transTime)))
		return uri, nil, ErrBigTimeDifference, nil
	}

	// find first transcoded segment time
	// transTime := mistGetTimeFromSegURI(mpullres[1].pl.Segments[0].URI)
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
				found = true
				for i := 0; i < si; i++ {
					shouldSkip[1] = append(shouldSkip[1], mistSessionRE.ReplaceAllString(mpullres[1].pl.Segments[i].URI, ""))
				}
				glog.Infof("Found! shoud skip %d transcoded segments (%+v)", si, shouldSkip[1])
				break
			}
		}
	}
	if !found {
		return uri, nil, ErrNoMatchingSegments, nil
	}
	return uri, shouldSkip, nil, nil
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

func Timedout(e error) bool {
	t, ok := e.(interface {
		Timeout() bool
	})
	return ok && t.Timeout() || (e != nil && strings.Contains(e.Error(), "Client.Timeout"))
}

func (mc *MistController) pullFirstTime(uri string) ([]string, error) {
	mpl, err := utils.DownloadMasterPlaylist(uri)
	if err != nil {
		return nil, err
	}
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
	pl                     *m3u8.MediaPlaylist
	firstSegmentParseError error
	err                    error
	standardDeviation      float64 // σ of durations of segments in the playlist
	i                      int
}

func (mc *MistController) pullMediaPL(userName, uri string, i int, out chan *plPullRes) (*m3u8.MediaPlaylist, error) {
	resp, err := mhttpClient.Do(uhttp.GetRequest(uri))
	if err != nil {
		glog.Infof("===== get error getting media playlist %s: %v", uri, err)
		out <- &plPullRes{err: err, i: i}
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		err := fmt.Errorf("Status error getting media playlist %s: %v (%s) body: %s", uri, resp.StatusCode, resp.Status, string(b))
		out <- &plPullRes{err: err, i: i}
		return nil, err
	}
	mpl, _ := m3u8.NewMediaPlaylist(100, 100)
	err = mpl.DecodeFrom(resp.Body, true)
	// err = mpl.Decode(*bytes.NewBuffer(b), true)
	resp.Body.Close()
	if err != nil {
		glog.Infof("Error getting media playlist uri=%s err=%v", uri, err)
		out <- &plPullRes{err: err, i: i}
		return nil, err
	}
	cs := countSegments(mpl)
	glog.V(model.VVERBOSE).Infof("Got media playlist with count=%d len=%d mc=%d segments (%s):", mpl.Count(), mpl.Len(), cs, uri)
	glog.V(model.VVERBOSE).Info(mpl)
	if cs < 1 {
		glog.Infof("Playlist for uri=%s has zero segments", uri)
		out <- &plPullRes{err: ErrZeroStreams, i: i}
		panic("no segments")
		// return nil, ErrZeroStreams
	}
	var verr error
	if i == 0 {
		segURI := mpl.Segments[mpl.Count()-1].URI
		purl, err := url.Parse(segURI)
		if err != nil {
			glog.Fatal(err)
		}
		mplu, _ := url.Parse(uri)
		if !purl.IsAbs() {
			segURI = mplu.ResolveReference(purl).String()
		}
		_, verr, _ = mc.downloadSegment(userName, segURI)
	}
	var j uint
	var mean, sd float64
	for j = 0; j < mpl.Count(); j++ {
		seg := mpl.Segments[j]
		if seg == nil {
			// something wrong
			break
		}
		// glog.V(model.VVERBOSE).Infof("Segment j=%d seqid=%d dur=%f name=%s", j, seg.SeqId, seg.Duration, seg.URI)
		mean += seg.Duration
	}
	mean /= float64(mpl.Count())
	for j = 0; j < mpl.Count(); j++ {
		seg := mpl.Segments[j]
		if seg == nil {
			// something wrong
			break
		}
		// glog.V(model.VVERBOSE).Infof("Segment j=%d seqid=%d dur=%f name=%s", j, seg.SeqId, seg.Duration, seg.URI)
		sd += math.Pow(seg.Duration-mean, 2)
	}
	sd = math.Sqrt(sd / float64(mpl.Count()))
	glog.V(model.VVERBOSE).Infof("SD=%f uri=%s", sd, uri)
	out <- &plPullRes{pl: mpl, i: i, firstSegmentParseError: verr, standardDeviation: sd}
	return mpl, nil
}

func (mc *MistController) downloadSegment(userName, uri string) ([]byte, error, error) {
	resp, err := httpClient.Do(uhttp.GetRequest(uri))
	if err != nil {
		glog.Infof("Error downloading video segment %s: %v", uri, err)
		return nil, nil, err
	}
	b, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		err := fmt.Errorf("Status error downloading media segment %s: %v (%s) body: %s", uri, resp.StatusCode, resp.Status, string(b))
		return nil, nil, err
	}
	if err != nil {
		glog.Errorf("Error downloading first segment uri=%s err=%v", uri, err)
		// panic(err)
		return nil, nil, err
	}
	fsttim, dur, keyFrames, _, verr := utils.GetVideoStartTimeDurFrames(b)
	glog.V(model.DEBUG).Infof("Downloaded segment %s pts=%s dur=%s keyFrames=%d len=%d verr=%v iseOF=%v", uri, fsttim, dur, keyFrames, len(b), verr, errors.Is(verr, io.EOF))
	if verr != nil && false {
		fname := "bad_video_seg_" + userName + ".ts"
		err = ioutil.WriteFile(fname, b, 0644)
		glog.Infof("Wrote bad segment to local file '%s' (err=%v)", fname, err)
		panic(err)
	}
	return b, verr, nil
}

type picartoSortedSegments []string

func (p picartoSortedSegments) Len() int { return len(p) }
func (p picartoSortedSegments) Less(i, j int) bool {
	return mistGetTimeFromSegURI(p[i]) < mistGetTimeFromSegURI(p[j])
}
func (p picartoSortedSegments) Swap(i, j int) { p[i], p[j] = p[j], p[i] }

func isFatalError(err error, try int) bool {
	// return err == ErrZeroStreams || err == ErrStreamOpenFailed || timedout(err) || errors.Is(err, io.EOF) || err == ErrNoAudioInStream
	return err == ErrZeroStreams || err == utils.ErrStreamOpenFailed || (Timedout(err) && try > 3) || err == ErrNoAudioInStream ||
		err == ErrBigTimeDifference || err == ErrNoMatchingSegments || errors.Is(err, ErrTooBigDurationsDeviation)
}
