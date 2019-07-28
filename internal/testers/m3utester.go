package testers

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/m3u8"
	"github.com/livepeer/stream-tester/internal/model"
	"github.com/livepeer/stream-tester/internal/utils"
)

// HTTPTimeout http timeout downloading manifests/segments
const HTTPTimeout = 2 * time.Second

var httpClient = &http.Client{
	// Transport: &http2.Transport{TLSClientConfig: tlsConfig},
	Timeout: HTTPTimeout,
}

// m3utester tests one stream, reading all the media streams
type m3utester struct {
	initialURL   *url.URL
	downloads    map[string]*mediaDownloader
	mu           sync.RWMutex
	started      bool
	finished     bool
	done         <-chan struct{} // signals to stop
	sentTimesMap *utils.SyncedTimesMap
}

// newM3UTester ...
func newM3UTester(done <-chan struct{}, sentTimesMap *utils.SyncedTimesMap) *m3utester {
	t := &m3utester{
		downloads:    make(map[string]*mediaDownloader),
		done:         done,
		sentTimesMap: sentTimesMap,
	}
	return t
}

func (mt *m3utester) IsFinished() bool {
	return mt.finished
}

func (mt *m3utester) Start(u string) {
	purl, err := url.Parse(u)
	if err != nil {
		glog.Fatal(err)
	}
	mt.initialURL = purl
	go mt.downloadLoop()
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

func (mt *m3utester) stats() downloadStats {
	stats := downloadStats{
		errors: make(map[string]int),
	}
	mt.mu.RLock()
	for _, d := range mt.downloads {
		stats.bytes += d.stats.bytes
		stats.success += d.stats.success
		stats.fail += d.stats.fail
		for e, en := range d.stats.errors {
			stats.errors[e] = stats.errors[e] + en
		}
	}
	mt.mu.RUnlock()
	return stats
}

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

func (mt *m3utester) downloadLoop() {
	surl := mt.initialURL.String()
	loops := 0

	for {
		select {
		case <-mt.done:
			return
		default:
		}
		resp, err := httpClient.Get(surl)
		if err != nil {
			glog.Error(err)
			time.Sleep(2 * time.Second)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			time.Sleep(2 * time.Second)
			continue
		}
		b, err := ioutil.ReadAll(resp.Body)
		// err = mpl.DecodeFrom(resp.Body, true)
		mpl := m3u8.NewMasterPlaylist()
		err = mpl.Decode(*bytes.NewBuffer(b), true)
		resp.Body.Close()
		if err != nil {
			glog.Error(err)
			time.Sleep(2 * time.Second)
			continue
		}
		// glog.Info("Got playlist:")
		// glog.Info(mpl)
		for i, variant := range mpl.Variants {
			// glog.Infof("Variant URI: %s", variant.URI)
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
			mt.mu.Lock()
			if _, ok := mt.downloads[mediaURL]; !ok {
				md := newMediaDownloader(mediaURL, mt.done, mt.sentTimesMap)
				mt.downloads[mediaURL] = md
				// md.source = strings.Contains(mediaURL, "source")
				md.source = i == 0
			}
			mt.mu.Unlock()
		}
		// glog.Info(string(b))
		time.Sleep(2 * time.Second)
		loops++
		if loops%2 == 0 {
			if glog.V(model.DEBUG) {
				fmt.Println(mt.StatsFormatted())
			}
		}
	}
}

type downloadStats struct {
	success int
	fail    int
	retries int
	bytes   int64
	errors  map[string]int
}

type downloadTask struct {
	url   string
	seqNo uint64
}

func (ds *downloadStats) formatForConsole() string {
	r := fmt.Sprintf(`Success: %7d
`, ds.success)
	return r
}

// mediaDownloader downloads all the segments from one media stream
// (it constanly reloads manifest, and downloads any segments found in manifest)
type mediaDownloader struct {
	u                  *url.URL
	stats              downloadStats
	downTasks          chan downloadTask
	mu                 sync.Mutex
	firstSegmentParsed bool
	firstSegmentTime   time.Duration
	saveSegmentsToDisk bool
	done               <-chan struct{} // signals to stop
	sentTimesMap       *utils.SyncedTimesMap
	latencies          []time.Duration
	source             bool
}

func newMediaDownloader(u string, done <-chan struct{}, sentTimesMap *utils.SyncedTimesMap) *mediaDownloader {
	pu, err := url.Parse(u)
	if err != nil {
		glog.Fatal(err)
	}
	md := &mediaDownloader{
		u:         pu,
		downTasks: make(chan downloadTask, 16),
		stats: downloadStats{
			errors: make(map[string]int),
		},
		done:         done,
		sentTimesMap: sentTimesMap,
	}
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

type downloadResult struct {
	status string
	bytes  int
	try    int
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
		resp, err := httpClient.Get(fsurl)
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
		resp.Body.Close()
		glog.V(model.DEBUG).Infof("Download %s result: %s len %d", fsurl, resp.Status, len(b))
		if !md.firstSegmentParsed && task.seqNo == 0 {
			fst, err := utils.GetVideoStartTime(b)
			if err != nil {
				glog.Fatal(err)
			}
			md.firstSegmentParsed = true
			md.firstSegmentTime = fst
		}
		if md.sentTimesMap != nil {
			fsttim, err := utils.GetVideoStartTime(b)
			if err != nil {
				glog.Fatal(err)
			}
			var latency time.Duration
			if st, has := md.sentTimesMap.GetTime(fsttim, fsurl); has {
				latency = now.Sub(st)
				md.latencies = append(md.latencies, latency)
			}
			// glog.Infof("== downloaded segment seeeqNo %d segment start time %s latency %s current time %s", task.seqNo, fsttim, latency, now)
		}

		if md.saveSegmentsToDisk {
			upts := strings.Split(task.url, "/")
			// fn := upts[len(upts)-2] + "-" + path.Base(task.url)
			fn := fmt.Sprintf("%s-%05d.ts", upts[len(upts)-2], task.seqNo)
			err = ioutil.WriteFile(fn, b, 0644)
			if err != nil {
				glog.Fatal(err)
			}
		}
		res <- downloadResult{status: resp.Status, bytes: len(b), try: try}
		return
	}
}

func (md *mediaDownloader) workerLoop() {
	seen := newStringRing(128)
	resultsCahn := make(chan downloadResult, 32) // http status or excpetion
	for {
		select {
		case <-md.done:
			return
		case res := <-resultsCahn:
			// md.mu.Lock()
			glog.V(model.VERBOSE).Infof("Got result %+v", res)
			md.stats.retries += res.try
			if res.status == "200 OK" {
				md.stats.success++
				md.stats.bytes += int64(res.bytes)
			} else {
				md.stats.fail++
				md.stats.errors[res.status] = md.stats.errors[res.status] + 1
			}
			// md.mu.Unlock()
		case task := <-md.downTasks:
			if seen.Contains(task.url) {
				continue
			}
			glog.V(model.VERBOSE).Infof("Got task to download: seqNo=%d url=%s", task.seqNo, task.url)
			seen.Add(task.url)
			go md.downloadSegment(&task, resultsCahn)
		}
	}
}

func (md *mediaDownloader) downloadLoop() {
	surl := md.u.String()
	for {
		select {
		case <-md.done:
			return
		default:
		}
		resp, err := httpClient.Get(surl)
		if err != nil {
			glog.Error(err)
			time.Sleep(1 * time.Second)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			time.Sleep(1 * time.Second)
			continue
		}
		b, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		// fmt.Println("-----################")
		// fmt.Println(string(b))
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
		// glog.Infof("Got medifa playlist of url %s:", surl)
		// glog.Info(pl)
		for i, segment := range pl.Segments {
			if segment != nil {
				// glog.Infof("Segment: %+v", *segment)

				md.downTasks <- downloadTask{url: segment.URI, seqNo: pl.SeqNo + uint64(i)}
				// glog.V(model.VERBOSE).Infof("segment %s is of length %f seqId=%d", segment.URI, segment.Duration, segment.SeqId)
			}
		}
		delay := 1 * time.Second
		if md.sentTimesMap != nil {
			delay = 250 * time.Millisecond
		}
		time.Sleep(delay)
	}
}

func getSortedKeys(data map[string]*mediaDownloader) []string {
	res := make(sort.StringSlice, 0, len(data))
	for k := range data {
		res = append(res, k)
	}
	res.Sort()
	return res
}
