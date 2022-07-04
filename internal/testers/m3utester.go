package testers

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/joy4/jerrors"
	"github.com/livepeer/m3u8"
	"github.com/livepeer/stream-tester/internal/utils"
	"github.com/livepeer/stream-tester/internal/utils/uhttp"
	"github.com/livepeer/stream-tester/messenger"
	"github.com/livepeer/stream-tester/model"
	"golang.org/x/net/http2"
)

const picartoDebug = false
const stopAfterMediaStreamsDrop = false

// HTTPTimeout http timeout downloading manifests/segments
const HTTPTimeout = 16 * time.Second

var httpClient = &http.Client{
	Timeout: HTTPTimeout,
}

var http2Client = &http.Client{
	Transport: &http2.Transport{},
	Timeout:   HTTPTimeout,
}

var wowzaSessionRE *regexp.Regexp = regexp.MustCompile(`_(w\d+)_`)
var mistSessionRE *regexp.Regexp = regexp.MustCompile(`(\?sessId=\d+)`)

type downStats2 struct {
	downSource            int
	downTransAll          int
	numProfiles           int
	sourceBytes           int64
	sourceTranscodedBytes int64 // downloaded bytes in source stream that matched with transcoded segments
	transAllBytes         int64
	downTrans             []int
	successRate           float64
	lastDownloadTime      time.Time
	videoParseErrors      int64
	audioCodecErrors      int64
	sourceDuration        time.Duration
	transcodedDuration    time.Duration
	url                   string
}

// m3utester tests one stream, reading all the media streams
type m3utester struct {
	ctx              context.Context
	initialURL       *url.URL
	name             string
	downloads        map[string]*mediaDownloader
	downloadsKeys    []string // first should be source
	mu               sync.RWMutex
	started          bool
	finished         bool
	wowzaMode        bool
	mistMode         bool
	picartoMode      bool
	infiniteMode     bool
	save             bool
	startTime        time.Time
	sentTimesMap     *utils.SyncedTimesMap
	segmentsMatcher  *segmentsMatcher
	fullResultsCh    chan *fullDownloadResult
	succ2mu          sync.Mutex
	downStats2       downStats2
	downSegs         map[string]map[string]*fullDownloadResult
	savePlayList     *m3u8.MasterPlaylist
	savePlayListName string
	saveDirName      string
	cancel           context.CancelFunc
	shouldSkip       [][]string
}

type fullDownloadResult struct {
	downloadResult
	mediaPlaylistName string
	resolution        string
	uri               string
}

type downloadResult struct {
	status             string
	bytes              int
	try                int
	videoParseError    error
	startTime          time.Duration
	duration           time.Duration
	appTime            time.Time
	timeAtFirstPlace   time.Time
	downloadStartedAt  time.Time
	downloadCompetedAt time.Time
	name               string
	seqNo              uint64
	mySeqNo            uint64
	resolution         string
	keyFrames          int
	task               *downloadTask
	data               []byte
}

func (r *downloadResult) String() string {
	return fmt.Sprintf("%10s %14s seq %3d: time %7s duration %7s size %7d bytes appearance time %s (%d)",
		r.resolution, r.name, r.seqNo, r.startTime, r.duration, r.bytes, r.appTime, r.appTime.UnixNano())
}

func (r *downloadResult) String2() string {
	return fmt.Sprintf("%10s %20s seq %3d: time %7s duration %7s first %s app %s",
		r.resolution, r.name, r.seqNo, r.startTime, r.duration, r.timeAtFirstPlace.Format(printTimeFormat), r.appTime.Format(printTimeFormat))
}

// const printTimeFormat = "2006-01-02T15:04:05"
const printTimeFormat = "2006-01-02T15:04:05.999999999"

func newM3UTester(ctx context.Context, sentTimesMap *utils.SyncedTimesMap, wowzaMode, mistMode,
	picartoMode, infiniteMode, save bool, sm *segmentsMatcher, shouldSkip [][]string, name string) *m3utester {

	t := &m3utester{
		ctx:             ctx,
		downloads:       make(map[string]*mediaDownloader),
		sentTimesMap:    sentTimesMap,
		wowzaMode:       wowzaMode,
		mistMode:        mistMode,
		picartoMode:     picartoMode,
		infiniteMode:    infiniteMode,
		save:            save,
		segmentsMatcher: sm,
		shouldSkip:      shouldSkip,
		name:            name,
		fullResultsCh:   make(chan *fullDownloadResult, 32),
		downSegs:        make(map[string]map[string]*fullDownloadResult),
	}
	ct, cancel := context.WithCancel(ctx)
	t.ctx = ct
	t.cancel = cancel
	if save {
		t.savePlayList = m3u8.NewMasterPlaylist()
	}
	return t
}

func (mt *m3utester) IsFinished() bool {
	return mt.finished
}

func (mt *m3utester) Stop() {
	if mt.cancel != nil {
		mt.cancel()
	}
}

func (mt *m3utester) Start(u string) {
	purl, err := url.Parse(u)
	if err != nil {
		glog.Fatal(err)
	}
	mt.initialURL = purl
	if mt.save {
		up := strings.Split(u, "/")
		upl := len(up)
		mt.saveDirName = ""
		mt.savePlayListName = up[upl-1]
		dashes := strings.Count(up[upl-2], "-")
		if upl > 1 {
			if up[upl-2] == "stream" {
				mt.saveDirName = strings.Split(up[upl-1], ".")[0] + "/"
			} else if mt.wowzaMode || mt.mistMode || dashes == 4 {
				mt.saveDirName = up[upl-2]
			}
		}
		if mt.saveDirName != "" {
			mt.savePlayListName = filepath.Join(mt.saveDirName, mt.savePlayListName)
			if _, err := os.Stat(mt.saveDirName); os.IsNotExist(err) {
				os.Mkdir(mt.saveDirName, 0755)
			}
		}
		glog.Infof("Save dir name: '%s', save playlist name %s", mt.saveDirName, mt.savePlayListName)
	}
	mt.downStats2.url = u
	go mt.manifestDownloadLoop()
	go mt.workerLoop()
}

func isTimeEqualM(t1, t2 time.Duration) bool {
	diff := t1 - t2
	if diff < 0 {
		diff *= -1
	}
	// 1000000
	return diff <= 100*time.Millisecond
}

func isTimeEqualTD(t1, t2 time.Time, d time.Duration) bool {
	diff := t1.Sub(t2)
	if diff < 0 {
		diff *= -1
	}
	return diff <= d
}

func absTimeTiff(t1, t2 time.Duration) time.Duration {
	diff := t1 - t2
	if diff < 0 {
		diff *= -1
	}
	return diff
}

// GetFIrstSegmentTime return timestamp of first frame of first segment.
// Second returned value is true if already found.
func (mt *m3utester) GetFIrstSegmentTime() (time.Duration, bool) {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	for _, md := range mt.downloads {
		if md.firstSegmentParsed {
			return md.firstSegmentTime, true
		}
	}
	return 0, false
}
func (mt *m3utester) statsSeparate() []*downloadStats {
	mt.mu.RLock()
	rs := make([]*downloadStats, 0, len(mt.downloads))
	for _, key := range mt.downloadsKeys {
		md := mt.downloads[key]
		md.mu.Lock()
		st := md.stats.clone()
		rs = append(rs, st)
		md.mu.Unlock()
	}
	mt.mu.RUnlock()
	return rs
}

func (mt *m3utester) stats() downloadStats {
	stats := downloadStats{
		errors: make(map[string]int),
	}
	mt.mu.RLock()
	for i, d := range mt.downloads {
		glog.V(model.DEBUG).Infof("==> for media stream %s succ %d fail %d", i, d.stats.success, d.stats.fail)
		d.mu.Lock()
		stats.bytes += d.stats.bytes
		stats.success += d.stats.success
		stats.fail += d.stats.fail
		if d.source {
			stats.keyframes = d.stats.keyframes
		}
		for e, en := range d.stats.errors {
			stats.errors[e] = stats.errors[e] + en
		}
		d.mu.Unlock()
	}
	mt.mu.RUnlock()
	return stats
}

func (mt *m3utester) getDownStats2() *downStats2 {
	mt.succ2mu.Lock()
	dr := mt.downStats2.clone()
	mt.succ2mu.Unlock()
	return dr
}

func (mt *m3utester) workerLoop() {
	c := time.NewTicker(8 * time.Second)
	lastVideoParseErrorSent := make(map[string]time.Time)
	for {
		select {
		case <-mt.ctx.Done():
			return
		case <-c.C:
			if len(mt.downloadsKeys) == 0 {
				continue
			}
			sourceKey := mt.downloadsKeys[0]
			if mt.downloads[sourceKey].isFinite {
				continue
			}
			mt.succ2mu.Lock()
			now := time.Now()
			glog.V(model.INSANE).Infof("=====>>>>>>>>>>>>>>>>>>>>>>>>>")
			glog.V(model.VVERBOSE).Infof("source key = %s, down stats2 %+v", sourceKey, mt.downStats2)
			for dk, dr := range mt.downSegs[sourceKey] {
				if now.Sub(dr.downloadCompetedAt) > 40*time.Second {
					mt.downStats2.downSource++
					mt.downStats2.sourceBytes += int64(dr.bytes)
					mt.downStats2.sourceDuration += dr.duration
					glog.V(model.INSANE).Infof("Checking source seg %s  pts %s down at %s", dk, dr.startTime, dr.downloadCompetedAt)
					someFound := false
					for i, transKey := range mt.downloadsKeys[1:] {
						transDM := mt.downSegs[transKey]
						found := false
						for transSegName, transSeg := range transDM {
							glog.V(model.INSANE).Infof("Checking %s pts %s down at %s", transSegName, transSeg.startTime, transSeg.downloadCompetedAt)
							if absTimeTiff(dr.startTime, transSeg.startTime) < 210*time.Millisecond {
								// match found
								mt.downStats2.downTransAll++
								mt.downStats2.downTrans[i]++
								mt.downStats2.transAllBytes += int64(transSeg.bytes)
								mt.downStats2.transcodedDuration += transSeg.duration
								delete(transDM, transSegName)
								found = true
								someFound = true
								break
							}
						}
						if !found {
							glog.V(model.VERBOSE).Infof("Not found pair for %s seg", dr.name)
						}
					}
					if someFound {
						mt.downStats2.sourceTranscodedBytes += int64(dr.bytes)
					} else {
						glog.V(model.VERBOSE).Infof("Source segment without matching transcoded one: name=%s uri=%s resolution=%s", dr.name, dr.uri, dr.resolution)
					}
					delete(mt.downSegs[sourceKey], dk)
					mt.downStats2.successRate = float64(mt.downStats2.downTransAll) / float64(mt.downStats2.downSource*mt.downStats2.numProfiles) * 100.0
				}
			}
			// todo: cleanup too old transcoded segments
			mt.succ2mu.Unlock()
		case fr := <-mt.fullResultsCh:
			mt.downSegs[fr.uri][fr.name] = fr
			mt.succ2mu.Lock()
			mt.downStats2.lastDownloadTime = fr.downloadCompetedAt
			if fr.videoParseError != nil {
				mt.downStats2.videoParseErrors++
				if errors.Is(fr.videoParseError, jerrors.ErrNoAudioInfoFound) {
					mt.downStats2.audioCodecErrors++
				}
			}
			mt.succ2mu.Unlock()
			if mt.picartoMode && fr.videoParseError != nil {
				if time.Since(lastVideoParseErrorSent[mt.initialURL.String()]) > 120*time.Second {
					// messenger.SendFatalMessage(fmt.Sprintf("Video parsing error for name=%s media manifest uri=%s err=%v", fr.name, fr.uri, fr.videoParseError))
					emsg := messenger.NewDiscordEmbed("Video parsing error for " + mt.name)
					emsg.AddField("Name", fr.name, true)
					emsg.AddField("Error", fr.videoParseError.Error(), true)
					emsg.URL = fr.uri
					// emsg.SetColorBySuccess(0.0)
					emsg.Color = 0xE1E412
					if !(IgnoreNoCodecError && isNoCodecError(fr.videoParseError)) {
						messenger.SendFatalRichMessage(emsg)
					} else {
						messenger.SendRichMessage(emsg)
					}
					lastVideoParseErrorSent[mt.initialURL.String()] = time.Now()
				}
			}

			if mt.save {
				err := ioutil.WriteFile(mt.savePlayListName, mt.savePlayList.Encode().Bytes(), 0644)
				if err != nil {
					glog.Fatal(err)
				}
			}
		}
	}
}

func (mt *m3utester) absVariantURI(variantURI string) string {
	pvrui, err := url.Parse(variantURI)
	if err != nil {
		glog.Error(err)
		panic(err)
	}
	// glog.Infof("Parsed uri: %+v", pvrui, pvrui.IsAbs)
	if !pvrui.IsAbs() {
		pvrui = mt.initialURL.ResolveReference(pvrui)
	}
	// glog.Info(pvrui)
	return pvrui.String()
}

func (mt *m3utester) waitForVODdownloads() {
	for {
		select {
		case <-mt.ctx.Done():
			return
		default:
		}
		allFinished := true
		for _, md := range mt.downloads {
			allFinished = allFinished && md.IsFiniteDownloadsFinished()
		}
		if allFinished {
			break
		}
		time.Sleep(4 * time.Second)
	}
	// todo run analysis here
	glog.Infof("Finished downloading VOD for %s", mt.initialURL.String())
	glog.V(model.SHORT).Infof("=====>>>>>>>>>>>>>>>>>>>>>>>>>")
	sourceKey := mt.downloadsKeys[0]
	glog.V(model.SHORT).Infof("source key = %s, down stats2 %+v", sourceKey, mt.downStats2)
	for dk, dr := range mt.downSegs[sourceKey] {
		mt.downStats2.downSource++
		mt.downStats2.sourceBytes += int64(dr.bytes)
		mt.downStats2.sourceDuration += dr.duration
		glog.V(model.INSANE).Infof("Checking source seg %s  pts %s down at %s", dk, dr.startTime, dr.downloadCompetedAt)
		someFound := false
		for i, transKey := range mt.downloadsKeys[1:] {
			transDM := mt.downSegs[transKey]
			found := false
			for transSegName, transSeg := range transDM {
				glog.V(model.INSANE2).Infof("Checking %s pts %s down at %s", transSegName, transSeg.startTime, transSeg.downloadCompetedAt)
				if absTimeTiff(dr.startTime, transSeg.startTime) < 210*time.Millisecond {
					// match found
					mt.downStats2.downTransAll++
					mt.downStats2.downTrans[i]++
					mt.downStats2.transAllBytes += int64(transSeg.bytes)
					mt.downStats2.transcodedDuration += transSeg.duration
					delete(transDM, transSegName)
					found = true
					someFound = true
					break
				}
			}
			if !found {
				glog.V(model.SHORT).Infof("Not found pair for %s seg in %s", dr.name, transKey)
			}
		}
		if someFound {
			mt.downStats2.sourceTranscodedBytes += int64(dr.bytes)
		} else {
			glog.V(model.SHORT).Infof("Source segment without matching transcoded one: name=%s uri=%s resolution=%s", dr.name, dr.uri, dr.resolution)
		}
		delete(mt.downSegs[sourceKey], dk)
		mt.downStats2.successRate = float64(mt.downStats2.downTransAll) / float64(mt.downStats2.downSource*mt.downStats2.numProfiles) * 100.0
	}
	mt.cancel()
}

func (mt *m3utester) manifestDownloadLoop() {
	surl := mt.initialURL.String()
	// loops := 0
	// var gotPlaylistWaitingForEnd bool
	var gotPlaylist bool
	period := 2 * time.Second
	if mt.picartoMode {
		period = 30 * time.Second
	}
	if mt.infiniteMode {
		glog.Infof("Waiting for playlist %s", surl)
	}
	mistMediaStreams := make(map[string]string) // maps clean urls to urls with session
	stopDeclared := false

	for {
		select {
		case <-mt.ctx.Done():
			return
		default:
		}
		if len(mt.downloadsKeys) > 0 {
			mt.mu.Lock()
			md := mt.downloads[mt.downloadsKeys[0]]
			if md.isFinite {
				go mt.waitForVODdownloads()
				mt.mu.Unlock()
				return
			}
			mt.mu.Unlock()
		}
		resp, err := httpClient.Do(uhttp.GetRequest(surl))
		if err != nil {
			glog.Infof("===== get error getting master playlist %s: %v", surl, err)
			time.Sleep(2 * time.Second)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			b, _ := ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			glog.Infof("===== status error getting master playlist %s: %v (%s) body: %s", surl, resp.StatusCode, resp.Status, string(b))
			time.Sleep(2 * time.Second)
			continue
		}
		b, err := ioutil.ReadAll(resp.Body)
		// err = mpl.DecodeFrom(resp.Body, true)
		mpl := m3u8.NewMasterPlaylist()
		err = mpl.Decode(*bytes.NewBuffer(b), true)
		resp.Body.Close()
		if err != nil {
			glog.Info("===== error getting master playlist: ", err)
			// glog.Error(err)
			time.Sleep(2 * time.Second)
			continue
		}
		glog.V(model.VVERBOSE).Infof("Got master playlist with %d variants (%s):", len(mpl.Variants), surl)
		glog.V(model.VVERBOSE).Info(mpl)
		if stopAfterMediaStreamsDrop && len(mpl.Variants) < 2 && !stopDeclared {
			glog.Infof("===== master playlist low on stream %s", surl)
			stopDeclared = true
			go func() {
				time.Sleep(10 * time.Second)
				panic("stop because of " + surl)
			}()
		}
		// glog.Infof("Got master playlist with %d variants (%s):", len(mpl.Variants), surl)
		// glog.Info(mpl)
		if mt.picartoMode && len(mpl.Variants) < 2 {
			emsg := fmt.Sprintf("For %s got playlist with %d variants, not starting download", surl, len(mpl.Variants))
			messenger.SendFatalMessage(emsg)
			mt.Stop()
			return
		}
		if !mt.wowzaMode || len(mpl.Variants) > model.ProfilesNum {
			if mt.infiniteMode {
				if !gotPlaylist {
					glog.Infof("Got playlist for %s with %d variants", surl, len(mpl.Variants))
					mt.startTime = time.Now()
					gotPlaylist = true
				}
			}
			// if len(mpl.Variants) > 1 && !gotPlaylistWaitingForEnd
			// gotPlaylistWaitingForEnd = true
			currentURLs := make(map[string]bool)
			for i, variant := range mpl.Variants {
				// glog.Infof("Variant URI: %s", variant.URI)
				if mt.wowzaMode {
					// remove Wowza's session id from URL
					variant.URI = wowzaSessionRE.ReplaceAllString(variant.URI, "_")
				}
				if mt.mistMode {
					vURIClean := mistSessionRE.ReplaceAllString(variant.URI, "")
					glog.V(model.VVERBOSE).Infof("Raw variant URI %s clean %s", variant.URI, vURIClean)
					if firstURI, has := mistMediaStreams[vURIClean]; has {
						variant.URI = firstURI
					} else {
						mistMediaStreams[vURIClean] = variant.URI
					}
				}
				glog.V(model.VVERBOSE).Infof("variant URI=%s", variant.URI)
				mediaURL := mt.absVariantURI(variant.URI)
				currentURLs[mediaURL] = true
				// Wowza changes media manifests on each fetch, so indentifying streams by
				// bandwitdh and
				// variantID := strconv.Itoa(variant.Bandwidth) + variant.Resolution
				mt.mu.Lock()
				glog.V(model.VERBOSE).Infof("mediaURL=%s downloads=%+v", mediaURL, mt.getDownloadsKeys())
				if _, ok := mt.downloads[mediaURL]; !ok {
					var shouldSkip []string
					if len(mt.shouldSkip) > i {
						shouldSkip = mt.shouldSkip[i]
					}
					md := newMediaDownloader(mt.ctx, mt.name, variant.URI, mediaURL, variant.Resolution, mt.sentTimesMap, mt.wowzaMode, mt.picartoMode, mt.save,
						mt.fullResultsCh, mt.saveDirName, mt.segmentsMatcher, shouldSkip)
					mt.downloads[mediaURL] = md
					// md.source = strings.Contains(mediaURL, "source")
					md.source = i == 0
					md.stats.source = md.source
					if mt.save {
						mt.savePlayList.Append(variant.URI, md.savePlayList, variant.VariantParams)
					}
					if len(mt.downloadsKeys) > 0 && md.source {
						panic(fmt.Sprintf("Source stream should be first, instead found %s, mediaURL=%s", mt.downloadsKeys[0], mediaURL))
					}
					mt.downloadsKeys = append(mt.downloadsKeys, mediaURL)
					mt.downSegs[mediaURL] = make(map[string]*fullDownloadResult)
					mt.downStats2.downTrans = append(mt.downStats2.downTrans, 0)
					mt.downStats2.numProfiles = len(mt.downloadsKeys) - 1
				}
				mt.mu.Unlock()
			}
			mt.mu.Lock()
			var toRemove []string
			for _, u := range mt.downloadsKeys {
				if _, has := currentURLs[u]; !has {
					md := mt.downloads[u]
					glog.Infof("Stream url=%s no longer in master playlist, removing from downloads", u)
					toRemove = append(toRemove, u)
					md.stop()
				}
			}
			if len(toRemove) > 0 {
				ok := mt.downloadsKeys
				mt.downloadsKeys = make([]string, 0, len(mt.downloadsKeys))
				for _, k := range ok {
					if !utils.StringsSliceContains(toRemove, k) {
						mt.downloadsKeys = append(mt.downloadsKeys, k)
					}
				}
			}
			mt.mu.Unlock()
		}
		time.Sleep(period)
	}
}

func (mt *m3utester) getDownloadsKeys() []string {
	r := make([]string, 0, len(mt.downloads))
	for k := range mt.downloads {
		r = append(r, k)
	}
	return r
}

func (ds2 *downStats2) clone() *downStats2 {
	r := *ds2
	r.downTrans = make([]int, len(ds2.downTrans))
	copy(r.downTrans, ds2.downTrans)
	return &r
}

func (ds2 *downStats2) discordRichMesage(title, hostSubstitute string, includeProfiles bool) *messenger.DiscordEmbed {
	emmsg := messenger.NewDiscordEmbed(title)
	emmsg.URL = ds2.url
	if hostSubstitute != "" && ds2.url != "" {
		pu, _ := url.Parse(ds2.url)
		port := pu.Port()
		pu.Host = hostSubstitute + ":" + port
		emmsg.URL = pu.String()
	}
	emmsg.SetColorBySuccess(ds2.successRate)
	emmsg.AddFieldF("Success rate", true, "**%7.4f%%**", ds2.successRate)
	emmsg.AddFieldF("Segments trans/source", true, "%d/%d", ds2.downTransAll, ds2.downSource)
	if includeProfiles {
		emmsg.AddFieldF("Num proflies", true, "%d", ds2.numProfiles)
	}
	emmsg.AddFieldF("Bytes downloaded", true, "%d/%d", ds2.transAllBytes, ds2.sourceBytes)
	var pob float64
	if ds2.sourceTranscodedBytes > 0 {
		pob = float64(ds2.transAllBytes) / float64(ds2.sourceTranscodedBytes) * 100
	}
	emmsg.AddFieldF("Percent of source bandwitdh", true, "**%4.2f%%**", pob)
	emmsg.AddFieldF("Video duration trans/source", true, "%s/%s", ds2.transcodedDuration, ds2.sourceDuration)
	if ds2.videoParseErrors > 0 {
		emmsg.AddFieldF("Number of video parse errors", true, "%d", ds2.videoParseErrors)
	}
	if ds2.audioCodecErrors > 0 {
		emmsg.AddFieldF("Number of audio codec errors", true, "%d", ds2.audioCodecErrors)
	}
	return emmsg
}

func (ds2 *downStats2) add(other *downStats2) {
	ds2.downSource += other.downSource
	ds2.downTransAll += other.downTransAll
	ds2.sourceBytes += other.sourceBytes
	ds2.sourceTranscodedBytes += other.sourceTranscodedBytes
	ds2.transAllBytes += other.transAllBytes
	ds2.videoParseErrors += other.videoParseErrors
	ds2.audioCodecErrors += other.audioCodecErrors
	ds2.sourceDuration += other.sourceDuration
	ds2.transcodedDuration += other.transcodedDuration
	ds2.numProfiles = other.numProfiles
	if ds2.downSource > 0 {
		ds2.successRate = float64(ds2.downTransAll) / float64(ds2.downSource*ds2.numProfiles) * 100.0
	}
	for i, v := range other.downTrans {
		if i < len(ds2.downTrans) {
			ds2.downTrans[i] += v
		}
	}
}
