package testers

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"../model"
	"github.com/ericxtang/m3u8"
	"github.com/golang/glog"
)

// m3utester tests one stream, reading all the media streams
type m3utester struct {
	initialURL *url.URL
	downloads  map[string]*mediaDownloader
	mu         sync.RWMutex
	started    bool
	finished   bool
}

// newM3UTester ...
// func newM3UTester() model.M3UTester {
func newM3UTester() *m3utester {
	t := &m3utester{
		downloads: make(map[string]*mediaDownloader),
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
		resp, err := http.Get(surl)
		if err != nil {
			glog.Fatal(err)
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
			glog.Fatal(err)
		}
		// glog.Info("Got playlist:")
		// glog.Info(mpl)
		for _, variant := range mpl.Variants {
			// glog.Infof("Variant URI: %s", variant.URI)
			pvrui, err := url.Parse(variant.URI)
			if err != nil {
				glog.Fatal(err)
			}
			// glog.Infof("Parsed uri: %+v", pvrui, pvrui.IsAbs)
			if !pvrui.IsAbs() {
				pvrui = mt.initialURL.ResolveReference(pvrui)
			}
			// glog.Info(pvrui)
			mediaURL := pvrui.String()
			mt.mu.Lock()
			if _, ok := mt.downloads[mediaURL]; !ok {
				md := newMediaDownloader(mediaURL)
				mt.downloads[mediaURL] = md
			}
			mt.mu.Unlock()
		}
		// glog.Info(string(b))
		time.Sleep(2 * time.Second)
		loops++
		if loops%2 == 0 {
			fmt.Println(mt.StatsFormatted())
		}
	}
}

type downloadStats struct {
	success int
	fail    int
	bytes   int64
	errors  map[string]int
}

func (ds *downloadStats) formatForConsole() string {
	r := fmt.Sprintf(`Success: %7d
`, ds.success)
	return r
}

type mediaDownloader struct {
	u         *url.URL
	stats     downloadStats
	downTasks chan string
	mu        sync.Mutex
	// successfulDownloads int
	// failedDownloads     int
	// bytesDownloaded     int64
	// errors              map[string]int
}

func newMediaDownloader(u string) *mediaDownloader {
	pu, err := url.Parse(u)
	if err != nil {
		glog.Fatal(err)
	}
	md := &mediaDownloader{
		u:         pu,
		downTasks: make(chan string, 16),
		stats: downloadStats{
			errors: make(map[string]int),
		},
	}
	go md.downloadLoop()
	go md.workerLoop()
	return md
}

func (md *mediaDownloader) statsFormatted() string {
	res := fmt.Sprintf("Downloaded: %5d\nFailed:     %5d\n", md.stats.success, md.stats.fail)
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
}

func (md *mediaDownloader) downloadSegment(surl string, res chan downloadResult) {
	purl, err := url.Parse(surl)
	if err != nil {
		glog.Fatal(err)
	}
	fsurl := surl
	if !purl.IsAbs() {
		fsurl = md.u.ResolveReference(purl).String()
	}
	glog.V(model.VERBOSE).Infof("Downloading segment %s", fsurl)
	resp, err := http.Get(fsurl)
	if err != nil {
		glog.Errorf("Error downloading %s: %v", fsurl, err)
		res <- downloadResult{status: err.Error()}
		return
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.Errorf("Error downloading reading body %s: %v", fsurl, err)
		res <- downloadResult{status: err.Error()}
		return
	}
	resp.Body.Close()
	glog.V(model.VERBOSE).Infof("Download %s result: %s len %d", fsurl, resp.Status, len(b))
	upts := strings.Split(surl, "/")
	fn := upts[len(upts)-2] + "-" + path.Base(surl)
	err = ioutil.WriteFile(fn, b, 0644)
	if err != nil {
		glog.Fatal(err)
	}
	res <- downloadResult{status: resp.Status, bytes: len(b)}
}

func (md *mediaDownloader) workerLoop() {
	seen := newStringRing(128)
	resultsCahn := make(chan downloadResult, 32) // http status or excpetion
	for {
		select {
		case res := <-resultsCahn:
			// md.mu.Lock()
			glog.V(model.VERBOSE).Infof("Got result %+v", res)
			if res.status == "200 OK" {
				md.stats.success++
				md.stats.bytes += int64(res.bytes)
			} else {
				md.stats.fail++
				md.stats.errors[res.status] = md.stats.errors[res.status] + 1
			}
			// md.mu.Unlock()
		case task := <-md.downTasks:
			if seen.Contains(task) {
				continue
			}
			glog.V(model.VERBOSE).Infof("Got task to download: %s", task)
			seen.Add(task)
			go md.downloadSegment(task, resultsCahn)
		}
	}
}

func (md *mediaDownloader) downloadLoop() {
	surl := md.u.String()
	for {
		resp, err := http.Get(surl)
		if err != nil {
			glog.Fatal(err)
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			time.Sleep(1 * time.Second)
			continue
		}
		// b, err := ioutil.ReadAll(resp.Body)
		// err = mpl.DecodeFrom(resp.Body, true)
		pl, err := m3u8.NewMediaPlaylist(100, 100)
		if err != nil {
			glog.Fatal(err)
		}
		// err = pl.Decode(*bytes.NewBuffer(b), true)
		err = pl.DecodeFrom(resp.Body, true)
		resp.Body.Close()
		if err != nil {
			glog.Fatal(err)
		}
		// glog.Infof("Got medifa playlist of url %s:", surl)
		// glog.Info(pl)
		for _, segment := range pl.Segments {
			if segment != nil {
				md.downTasks <- segment.URI
			}
		}
		time.Sleep(1 * time.Second)
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
