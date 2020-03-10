package testers

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/m3u8"
	"github.com/livepeer/stream-tester/internal/utils"
	"github.com/livepeer/stream-tester/internal/utils/uhttp"
	"github.com/livepeer/stream-tester/messenger"
	"github.com/livepeer/stream-tester/model"
	"golang.org/x/net/http2"
)

const picartoDebug = false

// HTTPTimeout http timeout downloading manifests/segments
const HTTPTimeout = 16 * time.Second

var httpClient = &http.Client{
	// Transport: &http2.Transport{TLSClientConfig: tlsConfig},
	// Transport: &http2.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: false}},
	// Transport: &http2.Transport{AllowHTTP: true},
	Timeout: HTTPTimeout,
}

var http2Client = &http.Client{
	// Transport: &http2.Transport{TLSClientConfig: tlsConfig},
	// Transport: &http2.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: false}},
	// Transport: &http2.Transport{AllowHTTP: true},
	Transport: &http2.Transport{},
	Timeout:   HTTPTimeout,
}

var wowzaSessionRE *regexp.Regexp = regexp.MustCompile(`_(w\d+)_`)
var wowzaBandwidthRE *regexp.Regexp = regexp.MustCompile(`_b(\d+)\.`)
var mistSessionRE *regexp.Regexp = regexp.MustCompile(`(\?sessId=\d+)`)

type downStats2 struct {
	downSource       int
	downTransAll     int
	numProfiles      int
	sourceBytes      int64
	transAllBytes    int64
	downTrans        []int
	successRate      float64
	lastDownloadTime time.Time
}

// m3utester tests one stream, reading all the media streams
type m3utester struct {
	initialURL       *url.URL
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
	done             <-chan struct{} // signals to stop
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
	// downloadResults  fullDownloadResultsMap
	// dm               sync.Mutex
	// gaps             int
}

/*
type fullDownloadResultsMap map[string]*fullDownloadResults

type fullDownloadResults struct {
	results           []downloadResult
	mediaPlaylistName string
	resolution        string
}

*/

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
	downloadCompetedAt time.Time
	name               string
	seqNo              uint64
	mySeqNo            uint64
	resolution         string
	keyFrames          int
}

func (r *downloadResult) String() string {
	// return fmt.Sprintf("%10s %14s seq %3d: mySeq %3d time %7s duration %7s size %7d bytes appearance time %s (%d)",
	// r.resolution, r.name, r.seqNo, r.mySeqNo, r.startTime, r.duration, r.bytes, r.appTime, r.appTime.UnixNano())
	return fmt.Sprintf("%10s %14s seq %3d: time %7s duration %7s size %7d bytes appearance time %s (%d)",
		r.resolution, r.name, r.seqNo, r.startTime, r.duration, r.bytes, r.appTime, r.appTime.UnixNano())
}

func (r *downloadResult) String2() string {
	// return fmt.Sprintf("%10s %14s seq %3d: mySeq %3d time %7s duration %7s size %7d bytes appearance at first place time %s (%d)",
	// 	r.resolution, r.name, r.seqNo, r.mySeqNo, r.startTime, r.duration, r.bytes, r.timeAtFirstPlace, r.timeAtFirstPlace.UnixNano())
	// return fmt.Sprintf("%10s %20s seq %3d: time %7s duration %7s size %7d bytes at first %s (%d)",
	// 	r.resolution, r.name, r.seqNo, r.startTime, r.duration, r.bytes, r.timeAtFirstPlace, r.timeAtFirstPlace.UnixNano())
	return fmt.Sprintf("%10s %20s seq %3d: time %7s duration %7s first %s app %s",
		r.resolution, r.name, r.seqNo, r.startTime, r.duration, r.timeAtFirstPlace.Format(printTimeFormat), r.appTime.Format(printTimeFormat))
}

// const printTimeFormat = "2006-01-02T15:04:05"
const printTimeFormat = "2006-01-02T15:04:05.999999999"

type downloadResultsBySeq []*downloadResult

func (p downloadResultsBySeq) Len() int { return len(p) }
func (p downloadResultsBySeq) Less(i, j int) bool {
	// return p[i].seqNo < p[j].seqNo
	// return p[i].mySeqNo < p[j].mySeqNo
	return p[i].appTime.Before(p[j].appTime)
}
func (p downloadResultsBySeq) Swap(i, j int) { p[i], p[j] = p[j], p[i] }
func (p downloadResultsBySeq) findBySeqNo(seqNo uint64) *downloadResult {
	for _, seg := range p {
		if seg.seqNo == seqNo {
			return seg
		}
	}
	return nil
}
func (p downloadResultsBySeq) findByMySeqNo(seqNo uint64) *downloadResult {
	for _, seg := range p {
		if seg.mySeqNo == seqNo {
			return seg
		}
	}
	return nil
}

// newM3UTester ...
func newM3UTester(ctx context.Context, done <-chan struct{}, sentTimesMap *utils.SyncedTimesMap, wowzaMode, mistMode,
	picartoMode, infiniteMode, save bool, sm *segmentsMatcher, shouldSkip [][]string) *m3utester {

	t := &m3utester{
		downloads:       make(map[string]*mediaDownloader),
		done:            done,
		sentTimesMap:    sentTimesMap,
		wowzaMode:       wowzaMode,
		mistMode:        mistMode,
		picartoMode:     picartoMode,
		infiniteMode:    infiniteMode,
		save:            save,
		segmentsMatcher: sm,
		shouldSkip:      shouldSkip,
		fullResultsCh:   make(chan *fullDownloadResult, 32),
		downSegs:        make(map[string]map[string]*fullDownloadResult),
		// downloadResults: make(map[string]*fullDownloadResults),
	}
	if ctx != nil {
		ct, cancel := context.WithCancel(ctx)
		t.done = ct.Done()
		t.cancel = cancel
	}
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
		if upl > 1 {
			if up[upl-2] == "stream" {
				mt.saveDirName = strings.Split(up[upl-1], ".")[0] + "/"
			} else if mt.wowzaMode || mt.mistMode {
				mt.saveDirName = up[upl-2]
			}
		}
		if mt.saveDirName != "" {
			mt.savePlayListName = path.Join(mt.saveDirName, mt.savePlayListName)
			if _, err := os.Stat(mt.saveDirName); os.IsNotExist(err) {
				os.Mkdir(mt.saveDirName, 0755)
			}
		}
		glog.Infof("Save dir name: '%s', save playlist name %s", mt.saveDirName, mt.savePlayListName)
	}
	go mt.downloadLoop()
	go mt.workerLoop()
	// if mt.infiniteMode {
	// 	go mt.anaylysePrintingLoop()
	// }
}

/*
func (mt *m3utester) anaylysePrintingLoop() string {
	for {
		time.Sleep(30 * time.Second)
		if !mt.startTime.IsZero() {
			mt.dm.Lock()
			a, _ := analyzeDownloads(mt.downloadResults, false, false)
			mt.dm.Unlock()
			glog.Infof("Analysis from start %s:\n%s", time.Since(mt.startTime), a)
		}
	}
}
*/

/*
func sortByResolution(results map[string]*fullDownloadResults) []string {
	r := make([]string, 0, len(results))
	return r
}

func containsString(ss []string, stf string) bool {
	for _, s := range ss {
		if s == stf {
			return true
		}
	}
	return false
}

func (fdr fullDownloadResultsMap) getResolutions() []string {
	res := make([]string, 0)
	for _, dr := range fdr {
		if !containsString(res, dr.resolution) {
			res = append(res, dr.resolution)
		}
	}
	return res
}

func (fdr fullDownloadResultsMap) byResolution(resolution string) []*fullDownloadResults {
	res := make([]*fullDownloadResults, 0)
	for _, dr := range fdr {
		if dr.resolution == resolution {
			res = append(res, dr)
		}
	}
	return res
}
*/

/*
  Should consider 10.867s and 10.866s to be equal
*/
func isTimeEqual(t1, t2 time.Duration) bool {
	diff := t1 - t2
	if diff < 0 {
		diff *= -1
	}
	// 1000000
	// return diff <= time.Millisecond
	return diff <= 1*time.Second
}

func isTimeEqualM(t1, t2 time.Duration) bool {
	diff := t1 - t2
	if diff < 0 {
		diff *= -1
	}
	// 1000000
	return diff <= 100*time.Millisecond
}

func isTimeEqualT(t1, t2 time.Time) bool {
	diff := t1.Sub(t2)
	if diff < 0 {
		diff *= -1
	}
	// 1000000
	return diff <= 10*time.Millisecond
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

func absTimeTiffT(t1, t2 time.Time) time.Duration {
	diff := t1.Sub(t2)
	if diff < 0 {
		diff *= -1
	}
	return diff
}

/*
func analyzeDownloads(downloadResults fullDownloadResultsMap, short, streamEnded bool) (string, int) {
	res := ""
	resolutions := downloadResults.getResolutions()
	byRes := make(map[string]downloadResultsBySeq)
	short = false
	var gaps int
	for _, resolution := range resolutions {
		results := make(downloadResultsBySeq, 0)
		fresults := downloadResults.byResolution(resolution)
		for _, rs := range fresults {
			for _, r := range rs.results {
				rl := r
				results = append(results, &rl)
			}
		}
		sort.Sort(results)
		byRes[resolution] = results
		res += fmt.Sprintf("=== For resolution %s:\n", resolution)
		allGood := "=== All is good! ===\n"
		if len(results) == 0 {
			res += "No segments!!!!\n"
			continue
		}
		if !short {
			res += fmt.Sprintf("==== Results sorted:\n")
			var tillNext time.Duration
			var problem string
			for i, r := range results {
				problem = ""
				tillNext = 0
				if i < len(results)-1 {
					ns := results[i+1]
					tillNext = ns.startTime - r.startTime
					if tillNext > 0 && !isTimeEqualM(r.duration, tillNext) {
						problem = fmt.Sprintf(" ===> possible gap - to big time difference %s", r.duration-tillNext)
					}
				}
				res += fmt.Sprintf("%10s %14s seq %3d: mySeq %3d time %s duration %s till next %s appearance time %s %s\n",
					resolution, r.name, r.seqNo, r.mySeqNo, r.startTime, r.duration, tillNext, r.appTime, problem)
			}
		}
		if results[0].seqNo > 1 {
			res += fmt.Sprintf("Segments start from %d\n", results[0].seqNo)
		}
		var lastSeq, lastRSeq uint64
		var lastStartTime time.Duration
		var lastFileName string
		for _, seg := range results {
			if seg.mySeqNo != lastSeq+1 {
				if seg.mySeqNo > lastSeq {
					res += fmt.Sprintf("Gap in sequence - file %s with seqNo %d mySeq %d (start time %s), previous seqNo is %d mySeq %d (start time %s)\n",
						seg.name, seg.seqNo, seg.mySeqNo, seg.startTime, lastSeq, lastRSeq, lastStartTime)
					allGood = ""
					gaps++
				} else if seg.mySeqNo == lastSeq {
					if seg.startTime != lastStartTime {
						res += fmt.Sprintf("Media stream switched, but corresponding segments have different time stamp: file %s with seqNo %d (start time %s), previous file %s seqNo is %d (start time %s)\n",
							seg.name, seg.seqNo, seg.startTime, lastFileName, lastSeq, lastStartTime)
						allGood = ""
					}
				} else if seg.mySeqNo < lastSeq {
					res += fmt.Sprintf("Very strange problem - seq is less than previous: file %s with seqNo %d (start time %s), previous seqNo is %d (start time %s)\n", seg.name, seg.seqNo, seg.startTime, lastSeq, lastStartTime)
					allGood = ""
				}
			}
			lastSeq = seg.mySeqNo
			lastRSeq = seg.seqNo
			lastStartTime = seg.startTime
			lastFileName = seg.name
		}
		res += allGood
	}
	// now check timestamps alignments in different renditions
	lastTimeDiffs := make(map[string]map[string]time.Duration)
	oneStep := make(map[string]map[string]bool)
	for resolution, resRes := range byRes {
		for i, seg := range resRes {
			for sresolution, resRes2 := range byRes {
				if sresolution == resolution {
					continue
				}
				if _, has := oneStep[resolution]; !has {
					oneStep[resolution] = make(map[string]bool)
				}
				if _, has := lastTimeDiffs[resolution]; !has {
					lastTimeDiffs[resolution] = make(map[string]time.Duration)
				}
				// altSeg := resRes2.findBySeqNo(seg.seqNo)
				altSeg := resRes2.findByMySeqNo(seg.mySeqNo)
				if altSeg == nil {
					if streamEnded || i < len(resRes)-4 {
						res += fmt.Sprintf("Segment %10s seqNo %3d mySeq %3d doesn't have corresponding segment in %10s\n", resolution, seg.seqNo, seg.mySeqNo, sresolution)
					}
				} else {
					if !isTimeEqual(seg.startTime, altSeg.startTime) {
						altSegM := resRes2.findBySeqNo(seg.seqNo - 1)
						altSegP := resRes2.findBySeqNo(seg.seqNo + 1)
						altSegM2 := resRes2.findBySeqNo(seg.seqNo - 2)
						altSegP2 := resRes2.findBySeqNo(seg.seqNo + 2)
						diff := seg.startTime - altSeg.startTime
						lastDiff := lastTimeDiffs[resolution][sresolution]
						if diff != lastDiff {
							// if !oneStep[resolution][sresolution] {
							res += fmt.Sprintf("Segment %10s seqNo %5d mySeq %3d has time %s but segment %10s seqNo %5d mySeq %3d has time %s diff %s\n",
								resolution, seg.seqNo, seg.mySeqNo, seg.startTime, sresolution, altSeg.seqNo, altSeg.mySeqNo, altSeg.startTime, seg.startTime-altSeg.startTime)
							// }
							if altSegM != nil && isTimeEqual(seg.startTime, altSegM.startTime) {
								if !oneStep[resolution][sresolution] {
									res += fmt.Sprintf("Stream %10s is one step behind stream %10s\n", resolution, sresolution)
									oneStep[resolution][sresolution] = true
									// break
								}
							}
							if altSegP != nil && isTimeEqual(seg.startTime, altSegP.startTime) {
								if !oneStep[resolution][sresolution] {
									res += fmt.Sprintf("Stream %10s is one step ahead stream %10s\n", resolution, sresolution)
									oneStep[resolution][sresolution] = true
									// break
								}
							}
							if altSegM2 != nil && isTimeEqual(seg.startTime, altSegM2.startTime) {
								if !oneStep[resolution][sresolution] {
									res += fmt.Sprintf("Stream %10s is two steps behind stream %10s\n", resolution, sresolution)
									oneStep[resolution][sresolution] = true
									// break
								}
							}
							if altSegP2 != nil && isTimeEqual(seg.startTime, altSegP2.startTime) {
								if !oneStep[resolution][sresolution] {
									res += fmt.Sprintf("Stream %10s is two steps ahead stream %10s\n", resolution, sresolution)
									oneStep[resolution][sresolution] = true
									// break
								}
							}
							// res += fmt.Sprintf("%d - %d\n", seg.startTime, altSeg.startTime)
							lastTimeDiffs[resolution][sresolution] = diff
						}
					}
				}
			}
		}
	}
	return res, gaps
}

type fullDownloadResultsArray []*fullDownloadResults

func (p fullDownloadResultsArray) Len() int { return len(p) }
func (p fullDownloadResultsArray) Less(i, j int) bool {
	ms1 := wowzaBandwidthRE.FindStringSubmatch(p[i].mediaPlaylistName)
	ms2 := wowzaBandwidthRE.FindStringSubmatch(p[j].mediaPlaylistName)
	// glog.Infof("name1 %s name2 %d res %+v res2 %+v", p[i].mediaPlaylistName, p[j].mediaPlaylistName, ms1, ms2)
	b1, _ := strconv.Atoi(ms1[1])
	b2, _ := strconv.Atoi(ms2[1])
	return b1 < b2
}
func (p fullDownloadResultsArray) Swap(i, j int) { p[i], p[j] = p[j], p[i] }

func sortByBandwidth(results map[string]*fullDownloadResults) fullDownloadResultsArray {
	res := make(fullDownloadResultsArray, 0, len(results))
	for _, r := range results {
		res = append(res, r)
	}
	sort.Sort(res)
	return res
}

func (mt *m3utester) DownloadStatsFormatted() string {
	mt.dm.Lock()
	defer mt.dm.Unlock()
	res := fmt.Sprintf("Has %d media playlists:\n", len(mt.downloadResults))
	sortedResults := sortByBandwidth(mt.downloadResults)
	for _, cdr := range sortedResults {
		res += fmt.Sprintf("Media playlist %25s resolution %10s segments %4d\n", cdr.mediaPlaylistName, cdr.resolution, len(cdr.results))
	}
	for _, cdr := range sortedResults {
		res += fmt.Sprintf("Media playlist %s:\n", cdr.mediaPlaylistName)
		for _, dr := range cdr.results {
			res += fmt.Sprintf("%s %s seqNo=%3d start time %s duration %s appearance time %s\n", cdr.resolution, dr.name, dr.seqNo, dr.startTime, dr.duration, dr.appTime)
			// startTime       time.Duration
			// duration        time.Duration
			// name            string
			// seqNo           uint64
		}
	}
	// res += "\nAnalysis:\n"
	// res += analyzeDownloads(mt.downloadResults)
	return res
}

func (mt *m3utester) AnalyzeFormatted(short bool) string {
	res := "\nAnalysis:\n"
	mt.dm.Lock()
	res1, _ := analyzeDownloads(mt.downloadResults, short, false)
	res += res1
	mt.dm.Unlock()
	return res
}
*/

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
	// mt.dm.Lock()
	// _, gaps := analyzeDownloads(mt.downloadResults, true, false)
	// stats.gaps = gaps
	// mt.dm.Unlock()
	return stats
}

/*
func (mt *m3utester) StatsFormatted() string {
	mt.mu.RLock()
	keys := getSortedKeys(mt.downloads)
	r := ""
	for _, u := range keys {
		d := mt.downloads[u]
		r += fmt.Sprintf("Stats for %s\n", u)
		r += d.statsFormatted()
	}
	mt.mu.RUnlock()
	return r
}
*/

func (mt *m3utester) getDownStats2() *downStats2 {
	mt.succ2mu.Lock()
	dr := mt.downStats2.clone()
	mt.succ2mu.Unlock()
	return dr
}

func (mt *m3utester) workerLoop() {
	c := time.NewTicker(8 * time.Second)
	for {
		select {
		case <-mt.done:
			return
		case <-c.C:
			if len(mt.downloadsKeys) == 0 {
				continue
			}
			mt.succ2mu.Lock()
			now := time.Now()
			sourceKey := mt.downloadsKeys[0]
			glog.V(model.INSANE).Infof("=====>>>>>>>>>>>>>>>>>>>>>>>>>")
			glog.V(model.VVERBOSE).Infof("source key = %s, down stats2 %+v", sourceKey, mt.downStats2)
			// glog.Infof("%+v", mt.downSegs)
			for dk, dr := range mt.downSegs[sourceKey] {
				if now.Sub(dr.downloadCompetedAt) > 16*time.Second {
					mt.downStats2.downSource++
					mt.downStats2.sourceBytes += int64(dr.bytes)
					glog.V(model.INSANE).Infof("Checking source seg %s  pts %s down at %s", dk, dr.startTime, dr.downloadCompetedAt)
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
								delete(transDM, transSegName)
								found = true
								break
							}
						}
						if !found {
							glog.V(model.VERBOSE).Infof("Not found pair for %s seg", dr.name)
						}
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
			mt.succ2mu.Unlock()
			if mt.picartoMode && fr.videoParseError != nil {
				messenger.SendFatalMessage(fmt.Sprintf("Video parsing error for uri=%s err=%v", fr.uri, fr.videoParseError))
			}

			// downSegs         map[string]map[string]*downloadResult
			/*
				mt.dm.Lock()
				if _, has := mt.downloadResults[fr.mediaPlaylistName]; !has {
					mt.downloadResults[fr.mediaPlaylistName] = &fullDownloadResults{resolution: fr.resolution, mediaPlaylistName: fr.mediaPlaylistName}
				}
				// turn off for now
				// r := mt.downloadResults[fr.mediaPlaylistName]
				// r.results = append(r.results, fr.downloadResult)
				mt.dm.Unlock()
			*/
			if mt.save {
				err := ioutil.WriteFile(mt.savePlayListName, mt.savePlayList.Encode().Bytes(), 0644)
				if err != nil {
					glog.Fatal(err)
				}
			}
		}
	}
}

func (mt *m3utester) downloadLoop() {
	surl := mt.initialURL.String()
	// loops := 0
	// var gotPlaylistWaitingForEnd bool
	var gotPlaylist bool
	if mt.infiniteMode {
		glog.Infof("Waiting for playlist %s", surl)
	}
	mistMediaStreams := make(map[string]string) // maps clean urls to urls with session

	for {
		select {
		case <-mt.done:
			return
		default:
		}
		/*
			if gotPlaylistWaitingForEnd {
				time.Sleep(2 * time.Second)
				loops++
				if loops%2 == 0 {
					if glog.V(model.DEBUG) {
						fmt.Println(mt.StatsFormatted())
					}
				}
				continue
			}
		*/
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
		// glog.Infof("Got master playlist with %d variants (%s):", len(mpl.Variants), surl)
		// glog.Info(mpl)
		if !mt.wowzaMode || len(mpl.Variants) > model.ProfilesNum {
			if mt.infiniteMode {
				if !gotPlaylist {
					glog.Infof("Got playlist for %s with %d variants", surl, len(mpl.Variants))
					mt.startTime = time.Now()
					gotPlaylist = true
				}
			}
			// if len(mpl.Variants) > 1 && !gotPlaylistWaitingForEnd {
			// gotPlaylistWaitingForEnd = true
			for i, variant := range mpl.Variants {
				// glog.Infof("Variant URI: %s", variant.URI)
				if mt.wowzaMode {
					// remove Wowza's session id from URL
					variant.URI = wowzaSessionRE.ReplaceAllString(variant.URI, "_")
				}
				if mt.mistMode {
					vURIClean := mistSessionRE.ReplaceAllString(variant.URI, "")
					glog.Infof("Raw variant URI %s clean %s", variant.URI, vURIClean)
					if firstURI, has := mistMediaStreams[vURIClean]; has {
						variant.URI = firstURI
					} else {
						mistMediaStreams[vURIClean] = variant.URI
					}
				}
				glog.Infof("variant URI=%s", variant.URI)
				pvrui, err := url.Parse(variant.URI)
				if err != nil {
					glog.Error(err)
					panic(err)
				}
				// glog.Infof("Parsed uri: %+v", pvrui, pvrui.IsAbs)
				if !pvrui.IsAbs() {
					pvrui = mt.initialURL.ResolveReference(pvrui)
				}
				// glog.Info(pvrui)
				mediaURL := pvrui.String()
				// Wowza changes media manifests on each fetch, so indentifying streams by
				// bandwitdh and
				// variantID := strconv.Itoa(variant.Bandwidth) + variant.Resolution
				mt.mu.Lock()
				glog.Infof("mediaURL=%s downloads=%+v", mediaURL, mt.getDownloadsKeys())
				if _, ok := mt.downloads[mediaURL]; !ok {
					var shouldSkip []string
					if len(mt.shouldSkip) > i {
						shouldSkip = mt.shouldSkip[i]
					}
					md := newMediaDownloader(variant.URI, mediaURL, variant.Resolution, mt.done, mt.sentTimesMap, mt.wowzaMode, mt.save,
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
			// glog.Infof("Processed playlist with %d variant, not checking anymore", len(mpl.Variants))
			// return
		}
		// }
		// glog.Info(string(b))
		time.Sleep(2 * time.Second)
		/*
			loops++
			if loops%2 == 0 {
				if glog.V(model.DEBUG) {
					fmt.Println(mt.StatsFormatted())
				}
			}
		*/
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

func (ds2 *downStats2) add(other *downStats2) {
	ds2.downSource += other.downSource
	ds2.downTransAll += other.downTransAll
	ds2.sourceBytes += other.sourceBytes
	ds2.transAllBytes += other.transAllBytes
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
