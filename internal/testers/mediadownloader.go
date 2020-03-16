package testers

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/joy4/jerrors"
	"github.com/livepeer/m3u8"
	"github.com/livepeer/stream-tester/internal/utils"
	"github.com/livepeer/stream-tester/internal/utils/uhttp"
	"github.com/livepeer/stream-tester/model"
)

type downloadStats struct {
	success    int
	fail       int
	retries    int
	keyframes  int
	bytes      int64
	errors     map[string]int
	resolution string
	source     bool
	// gaps      int
}

type downloadTask struct {
	baseURL  *url.URL
	url      string
	seqNo    uint64
	title    string
	duration float64
	mySeqNo  uint64
	appTime  time.Time
}

func (ds *downloadStats) formatForConsole() string {
	r := fmt.Sprintf(`Success: %7d
`, ds.success)
	return r
}

// mediaDownloader downloads all the segments from one media stream
// (it constanly reloads manifest, and downloads any segments found in manifest)
type mediaDownloader struct {
	parentName         string
	name               string // usually medial playlist relative name
	resolution         string
	u                  *url.URL
	suri               string
	stats              downloadStats
	downTasks          chan downloadTask
	mu                 sync.Mutex
	firstSegmentParsed bool
	firstSegmentTime   time.Duration
	firstSegmentTimes  sortedTimes     // PTSs of first segments
	done               <-chan struct{} // signals to stop
	sentTimesMap       *utils.SyncedTimesMap
	latencies          []time.Duration // latencies stored as segments get downloaded
	latenciesPerStream []time.Duration // here index is seqNo, so if segment is failed download then value will be zero
	source             bool
	wowzaMode          bool
	shouldSkip         []string
	saveSegmentsToDisk bool
	savePlayList       *m3u8.MediaPlaylist
	savePlayListName   string
	saveDir            string
	livepeerNameSchema bool
	fullResultsCh      chan *fullDownloadResult
	segmentsMatcher    *segmentsMatcher
	lastKeyFramesPTSs  sortedTimes
	downloadedSegments []string // for debugging
}

func newMediaDownloader(parentName, name, u, resolution string, done <-chan struct{}, sentTimesMap *utils.SyncedTimesMap, wowzaMode, save bool, frc chan *fullDownloadResult,
	baseSaveDir string, sm *segmentsMatcher, shouldSkip []string) *mediaDownloader {
	pu, err := url.Parse(u)
	if err != nil {
		glog.Fatal(err)
	}
	md := &mediaDownloader{
		parentName:      parentName,
		name:            name,
		u:               pu,
		suri:            u,
		resolution:      resolution,
		segmentsMatcher: sm,
		shouldSkip:      shouldSkip,
		downTasks:       make(chan downloadTask, 256),
		stats: downloadStats{
			errors:     make(map[string]int),
			resolution: resolution,
		},
		done:               done,
		sentTimesMap:       sentTimesMap,
		wowzaMode:          wowzaMode,
		saveSegmentsToDisk: save,
		fullResultsCh:      frc,
	}
	if save {
		mpl, err := m3u8.NewMediaPlaylist(10000, 10000)
		mpl.MediaType = m3u8.VOD
		mpl.Live = false
		if err != nil {
			panic(err)
		}
		md.savePlayList = mpl
		// md.savePlayListName = up[upl-1]
		md.saveDir = baseSaveDir
		// md.savePlayListName = path.Join(baseSaveDir, up[upl-1])
		if strings.Contains(name, baseSaveDir) {
			md.savePlayListName = name
		} else {
			base, _ := path.Split(md.savePlayListName)
			md.savePlayListName = path.Join(baseSaveDir, name)
			if base != "" {
				md.saveDir = path.Join(baseSaveDir, base)
			}
		}
		glog.V(model.DEBUG).Infof("Media stream %s (%s) save dir %s palylist name %s", name, resolution, md.saveDir, md.savePlayListName)
		up := strings.Split(u, "/")
		upl := len(up)
		if upl > 2 && up[upl-3] == "stream" {
			md.livepeerNameSchema = true
			// dirName = strings.Split(up[upl-1], ".")[0] + "/"
		}
		base, _ := path.Split(md.savePlayListName)
		if base != "" {
			if _, err := os.Stat(base); os.IsNotExist(err) {
				os.Mkdir(base, 0755)
			}
		}
		// if dirName != "" {
		// 	if _, err := os.Stat(dirName); os.IsNotExist(err) {
		// 		os.Mkdir(path.Join(baseSaveDir, dirName), 0755)
		// 	}
		// }
		// md.savePlayListName = path.Join(baseSaveDir, dirName, md.savePlayListName)
	}
	// md.saveSegmentsToDisk = true
	go md.downloadLoop()
	go md.workerLoop()
	return md
}

func (md *mediaDownloader) statsFormatted() string {
	res := fmt.Sprintf("Downloaded: %5d\nFailed:     %5d\nRetries:   %5d\n", md.stats.success, md.stats.fail, md.stats.retries)
	et := 0
	for _, e := range md.stats.errors {
		et += e
	}
	res += fmt.Sprintf("Errors: (%d total)\n", et)
	for en, ec := range md.stats.errors {
		res += fmt.Sprintf("Error %s: %d\n", en, ec)
	}
	return res
}

func (md *mediaDownloader) downloadSegment(task *downloadTask, res chan downloadResult) {
	purl, err := url.Parse(task.url)
	if err != nil {
		glog.Fatal(err)
	}
	fsurl := task.url
	if !purl.IsAbs() {
		fsurl = md.u.ResolveReference(purl).String()
	}
	try := 0
	for {
		glog.V(model.DEBUG).Infof("Downloading segment seqNo=%d url=%s try=%d", task.seqNo, fsurl, try)
		resp, err := httpClient.Do(uhttp.GetRequest(fsurl))
		if err != nil {
			glog.Errorf("Error downloading %s: %v", fsurl, err)
			if try < 4 {
				try++
				continue
			}
			res <- downloadResult{status: err.Error(), try: try}
			return
		}
		b, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		now := time.Now()
		if err != nil {
			glog.Errorf("Error downloading reading body %s: %v", fsurl, err)
			if try < 4 {
				try++
				continue
			}
			res <- downloadResult{status: err.Error(), try: try}
			return
		}
		if resp.StatusCode != http.StatusOK {
			glog.Errorf("Error status downloading segment %s result status code %d status %s", fsurl, resp.StatusCode, resp.Status)
			if try < 8 && resp.StatusCode != http.StatusNotFound {
				try++
				time.Sleep(time.Second)
				continue
			}
			res <- downloadResult{status: resp.Status, try: try}
			return
		}
		fsttim, dur, keyFrames, skeyFrames, verr := utils.GetVideoStartTimeDurFrames(b)
		if verr != nil {
			msg := fmt.Sprintf("Error parsing video data %s result status %s video data len %d err %v", fsurl, resp.Status, len(b), err)
			glog.Error(msg)
			if model.FailHardOnBadSegments && !(IgnoreNoCodecError && (errors.Is(verr, jerrors.ErrNoAudioInfoFound) || errors.Is(verr, jerrors.ErrNoVideoInfoFound))) {
				fname := fmt.Sprintf("bad_video_%s_%s_%d.ts", utils.CleanFileName(md.parentName), utils.CleanFileName(md.name), task.seqNo)
				err = ioutil.WriteFile(fname, b, 0644)
				glog.Infof("Wrote bad segment to '%s' (err=%v)", fname, err)
				panic(verr)
			}
		} else {
			// add keys
			md.mu.Lock()
			for _, tm := range skeyFrames {
				md.lastKeyFramesPTSs = append(md.lastKeyFramesPTSs, tm)
			}
			sort.Sort(md.lastKeyFramesPTSs)
			if len(md.lastKeyFramesPTSs) > 32 {
				md.lastKeyFramesPTSs = md.lastKeyFramesPTSs[len(md.lastKeyFramesPTSs)-32:]
			}
			md.mu.Unlock()
		}
		glog.V(model.DEBUG).Infof("Download %s result: %s len %d timeStart %s segment duration %s keyframes %d (%+v)",
			fsurl, resp.Status, len(b), fsttim, dur, keyFrames, skeyFrames)
		if !md.firstSegmentParsed && task.seqNo == 0 {
			// fst, err := utils.GetVideoStartTime(b)
			// if err != nil {
			// 	glog.Fatal(err)
			// }
			md.firstSegmentTime = fsttim
			md.firstSegmentParsed = true
		}
		if len(md.firstSegmentTimes) < 32 {
			md.mu.Lock()
			md.firstSegmentTimes = append(md.firstSegmentTimes, fsttim)
			sort.Sort(md.firstSegmentTimes)
			md.mu.Unlock()
		}
		if md.segmentsMatcher != nil {
			// fsttim, dur, err := utils.GetVideoStartTimeAndDur(b)
			// if err != nil {
			// 	if err != io.EOF {
			// 		glog.Fatal(err)
			// 	}
			// } else {
			if verr == nil {
				latency, speedRatio, merr := md.segmentsMatcher.matchSegment(fsttim, dur, now)
				src := "    source"
				if !md.source {
					src = "transcoded"
				}
				glog.V(model.DEBUG).Infof("== downloaded %s seqNo %d start time %s lat %s now %s speed ratio %v merr %v", src, task.seqNo, fsttim, latency, now, speedRatio, merr)
				if merr == nil {
					md.mu.Lock()
					md.latencies = append(md.latencies, latency)
					for len(md.latenciesPerStream) <= int(task.seqNo) {
						md.latenciesPerStream = append(md.latenciesPerStream, 0)
					}
					md.latenciesPerStream[task.seqNo] = latency
					glog.V(model.VVERBOSE).Infof("lat: %+v", md.latenciesPerStream)
					md.mu.Unlock()
				}
				if merr != nil {
					panic(merr)
				}
				/*
					var st time.Time
					var has bool
					if st, has = md.sentTimesMap.GetTime(fsttim, fsurl); has {
						latency = now.Sub(st)
						md.latencies = append(md.latencies, latency)
						md.mu.Lock()
						for len(md.latenciesPerStream) <= int(task.seqNo) {
							md.latenciesPerStream = append(md.latenciesPerStream, 0)
						}
						md.latenciesPerStream[task.seqNo] = latency
						md.mu.Unlock()
					}
					latency2, speedRatio, merr := md.segmentsMatcher.matchSegment(fsttim, dur, now)
					glog.Infof("== downloaded %s seqNo %d start time %s lat %s mlat %s now %s lat found %v sr %v merr %v",
						src, task.seqNo, fsttim, latency, latency2, now, has, speedRatio, merr)
				*/
			}
		}

		if md.saveSegmentsToDisk {
			seg := new(m3u8.MediaSegment)
			seg.URI = task.url
			seg.SeqId = task.seqNo
			seg.Duration = task.duration
			seg.Title = task.title
			// md.savePlayList.AppendSegment(seg)
			md.mu.Lock()
			md.savePlayList.InsertSegment(task.seqNo, seg)
			md.mu.Unlock()

			// glog.Infof("url: %s", task.url)
			upts := strings.Split(fsurl, "/")
			// fn := upts[len(upts)-2] + "-" + path.Base(task.url)
			ind := len(upts) - 2
			fn := path.Base(task.url)
			if !md.livepeerNameSchema {
				// ind = 0
				// fn = upts[0]
				for i, n := range upts {
					if n == md.saveDir {
						fn = strings.Join(upts[i+1:], "/")
						break
					}
				}
			} else {
				// fn := fmt.Sprintf("%s-%05d.ts", upts[ind], task.seqNo)
				fn = fmt.Sprintf("%s-%s", upts[ind], fn)
			}
			glog.V(model.INSANE).Infof("Saving segment url=%s fsurl=%s saveDir=%s fn=%s livepeerNameSchema=%v", task.url, fsurl, md.saveDir, fn, md.livepeerNameSchema)
			// playListFileName := md.name
			// if md.wowzaMode {
			// 	dn := upts[len(upts)-2]
			// 	if _, err := os.Stat(dn); os.IsNotExist(err) {
			// 		os.Mkdir(dn, 0755)
			// 	}
			// 	fn = path.Join(dn, upts[len(upts)-1])
			// 	playListFileName = path.Join(dn, playListFileName)
			// }
			err = ioutil.WriteFile(path.Join(md.saveDir, fn), b, 0644)
			if err != nil {
				glog.Fatal(err)
			}
			md.mu.Lock()
			err = ioutil.WriteFile(md.savePlayListName, md.savePlayList.Encode().Bytes(), 0644)
			md.mu.Unlock()
			if err != nil {
				glog.Fatal(err)
			}
			glog.V(model.DEBUG).Infof("Segment %s saved to %s", seg.URI, path.Join(md.saveDir, fn))
		}
		res <- downloadResult{status: resp.Status, bytes: len(b), try: try, name: task.url, seqNo: task.seqNo, downloadCompetedAt: now,
			videoParseError: verr, startTime: fsttim, duration: dur, mySeqNo: task.mySeqNo, appTime: task.appTime, keyFrames: keyFrames}
		return
	}
}

func (md *mediaDownloader) workerLoop() {
	// seen := newStringRing(128 * 1024)
	resultsCahn := make(chan downloadResult, 32) // http status or excpetion
	for {
		select {
		case <-md.done:
			return
		case res := <-resultsCahn:
			md.mu.Lock()
			glog.V(model.VERBOSE).Infof("Got result %+v", res)
			glog.V(model.DEBUG).Infof("Got download result seqNo=%d start time=%s dur=%s keys=%d res=%s name=%s", res.seqNo,
				res.startTime, res.duration, res.keyFrames, md.resolution, res.name)
			md.stats.retries += res.try
			if res.status == "200 OK" {
				uriClean := mistSessionRE.ReplaceAllString(res.name, "")
				md.stats.success++
				md.stats.keyframes += res.keyFrames
				md.stats.bytes += int64(res.bytes)
				md.fullResultsCh <- &fullDownloadResult{downloadResult: res, mediaPlaylistName: md.name, resolution: md.resolution, uri: md.suri}
				if picartoDebug {
					md.downloadedSegments = append(md.downloadedSegments, uriClean)
				}
			} else {
				md.stats.fail++
				md.stats.errors[res.status] = md.stats.errors[res.status] + 1
			}
			md.mu.Unlock()
		case task := <-md.downTasks:
			// if seen.Contains(task.url) {
			// 	continue
			// }
			glog.V(model.VERBOSE).Infof("Got task to download: seqNo=%d url=%s", task.seqNo, task.url)
			// seen.Add(task.url)
			go md.downloadSegment(&task, resultsCahn)
		}
	}
}

func (md *mediaDownloader) downloadLoop() {
	surl := md.u.String()
	gotManifest := false
	var mySeqNo uint64
	seen := newStringRing(128 * 1024)
	for {
		select {
		case <-md.done:
			return
		default:
		}
		resp, err := httpClient.Do(uhttp.GetRequest(surl))
		if err != nil {
			glog.Error(err)
			time.Sleep(1 * time.Second)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode != http.StatusNotFound {
				glog.Infof("Media playlist %s resolution %s status %v: %v", surl, md.resolution, resp.StatusCode, resp.Status)
			} else {
				glog.Infof("Media playlist %s resolution %s status %v: %v", surl, md.resolution, resp.StatusCode, resp.Status)
			}
			time.Sleep(1 * time.Second)
			continue
		}
		b, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			glog.Infof("Media playlist %s resolution %s mpl read error %v", surl, md.resolution, err)
			time.Sleep(time.Second)
			continue
		}
		// if !md.source {
		// 	fmt.Println("-----################")
		// 	fmt.Println(string(b))
		// }
		// glog.Infoln("-----################")
		// glog.Infoln(string(b))
		// err = mpl.DecodeFrom(resp.Body, true)
		pl, err := m3u8.NewMediaPlaylist(100, 100)
		if err != nil {
			glog.Fatal(err)
		}
		err = pl.Decode(*bytes.NewBuffer(b), true)
		// err = pl.DecodeFrom(resp.Body, true)
		// resp.Body.Close()
		if err != nil {
			glog.Fatal(err)
		}
		glog.V(model.INSANE).Infof("Got media playlist %s with %d (really %d) segments of url %s:", md.resolution, len(pl.Segments), countSegments(pl), surl)
		glog.V(model.INSANE).Info(pl)
		if !gotManifest && md.saveSegmentsToDisk {
			md.savePlayList.TargetDuration = pl.TargetDuration
			md.savePlayList.SeqNo = pl.SeqNo
			gotManifest = true
		}
		// for i := len(pl.Segments) - 1; i >= 0; i-- {
		now := time.Now()
		for i, segment := range pl.Segments {
			// segment := pl.Segments[i]
			if segment != nil {
				// glog.Infof("Segment: %+v", *segment)
				if md.wowzaMode {
					// remove Wowza's session id from URL
					segment.URI = wowzaSessionRE.ReplaceAllString(segment.URI, "_")
				}
				if seen.Contains(segment.URI) {
					continue
				}
				if len(md.shouldSkip) > 0 {
					curi := mistSessionRE.ReplaceAllString(segment.URI, "")
					if utils.StringsSliceContains(md.shouldSkip, curi) {
						seen.Add(segment.URI)
						mySeqNo++
						glog.Infof("Skippingg %s (%s) %s", md.name, md.resolution, segment.URI)
						continue
					}
				}
				seen.Add(segment.URI)
				mySeqNo++
				seqNo := pl.SeqNo + uint64(i)
				// attempt to parse seqNo from file name
				_, fn := path.Split(segment.URI)
				ext := path.Ext(fn)
				fn = strings.TrimSuffix(fn, ext)
				parsedSeq, err := strconv.ParseUint(fn, 10, 64)
				if err == nil {
					seqNo = parsedSeq
				}
				md.downTasks <- downloadTask{url: segment.URI, seqNo: seqNo, title: segment.Title, duration: segment.Duration, mySeqNo: mySeqNo, appTime: now}
				now = now.Add(time.Millisecond)
				// glog.V(model.VERBOSE).Infof("segment %s is of length %f seqId=%d", segment.URI, segment.Duration, segment.SeqId)
			}
		}
		delay := 1 * time.Second
		if md.sentTimesMap != nil || md.segmentsMatcher != nil {
			delay = 100 * time.Millisecond
		}
		time.Sleep(delay)
	}
}

func countSegments(mpl *m3u8.MediaPlaylist) int {
	var res int
	for _, seg := range mpl.Segments {
		if seg != nil {
			res++
		}
	}
	return res
}

/*
func getSortedKeys(data map[string]*mediaDownloader) []string {
	res := make(sort.StringSlice, 0, len(data))
	for k := range data {
		res = append(res, k)
	}
	res.Sort()
	return res
}
*/

type sortedTimes []time.Duration

func (p sortedTimes) Len() int { return len(p) }
func (p sortedTimes) Less(i, j int) bool {
	return p[i] < p[j]
}
func (p sortedTimes) Swap(i, j int) { p[i], p[j] = p[j], p[i] }

func (ds *downloadStats) clone() *downloadStats {
	st := *ds
	st.errors = make(map[string]int)
	for k, v := range ds.errors {
		st.errors[k] = v
	}
	return &st
}
