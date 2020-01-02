package testers

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/stream-tester/internal/messenger"
	"github.com/livepeer/stream-tester/internal/utils"
)

// Uploader draft for common interface for both RTMP and HTTP uploaders
type Uploader interface {
	Stop()
	StartUpload(fileName, url string, segmentsToStream int, waitForTarget time.Duration)
}

// httpStreamer takes video file, cuts it into segments
// and streams into Livepeer broadcaster using HTTP ingest
// then it reads resutls back, check if video segments are
// parseable, and calculates transcode latecies and succes rate
type httpStreamer struct {
	done          chan struct{}
	saveLatencies bool
	dstats        httpStats
	mu            sync.RWMutex
}

type httpStats struct {
	triedToSend       int
	sent              int
	failedToSend      int
	downloaded        int
	downloadFailures  int
	transcodeFailures int
	success           int
	bytes             int64
	errors            map[string]int
	latencies         []time.Duration
}

func newHTTPtreamer(done chan struct{}, saveLatencies bool) *httpStreamer {
	hs := &httpStreamer{done: done, saveLatencies: saveLatencies}
	hs.dstats.errors = make(map[string]int)
	return hs
}

func (hs *httpStreamer) Stop() {

}

// var savePrefix = "segmented3"
var savePrefix = ""

// StartUpload starts HTTP segments. Blocks until end.
func (hs *httpStreamer) StartUpload(fn, httpURL, manifestID string, segmentsToStream int, waitForTarget, stopAfter time.Duration) {
	segmentsIn := make(chan *hlsSegment)
	err := startSegmenting(fn, true, stopAfter, segmentsIn)
	if err != nil {
		glog.Infof("Error starting segmenter: %v", err)
		panic(err)
	}
outloop:
	for seg := range segmentsIn {
		select {
		case <-hs.done:
			glog.Infof("=========>>>> got stop singal")
			// rs.file.Close()
			// conn.Close()
			// return
			break outloop
		default:
		}
		if seg.err != nil {
			glog.Infof("Error during segmenting: %v", seg.err)
			break
		}
		go hs.pushSegment(httpURL, manifestID, seg)
	}
	hs.closeDone()
}

func (hs *httpStreamer) pushSegment(httpURL, manifestID string, seg *hlsSegment) {
	urlToUp := fmt.Sprintf("%s/%d.ts", httpURL, seg.seqNo)
	glog.Infof("Got segment manifes %s seqNo %d pts %s dur %s bytes %d from segmenter, uploading to %s", manifestID, seg.seqNo, seg.pts, seg.duration, len(seg.data), urlToUp)
	var body io.Reader
	body = bytes.NewReader(seg.data)
	req, err := http.NewRequest("POST", urlToUp, body)
	if err != nil {
		panic(err)
	}
	req.Header.Set("Accept", "multipart/mixed")
	req.Header.Set("Content-Duration", strconv.FormatInt(seg.duration.Milliseconds(), 10))
	postStarted := time.Now()
	resp, err := httpClient.Do(req)
	postTook := time.Since(postStarted)
	var timedout bool
	var status string
	if err != nil {
		uerr := err.(*url.Error)
		timedout = uerr.Timeout()
	}
	if resp != nil {
		status = resp.Status
	}
	glog.Infof("Post segment manifest %s seqNo %d pts %s dur %s took %s timed out %v status '%v'", manifestID, seg.seqNo, seg.pts, seg.duration, postTook, timedout, status)
	if err != nil {
		hs.mu.Lock()
		hs.dstats.triedToSend++
		hs.dstats.failedToSend++
		hs.mu.Unlock()
		return
		// panic(err)
	}
	glog.Infof("Got manifest %s resp status %s reading body started", manifestID, resp.Status)
	if resp.StatusCode != http.StatusOK {
		b, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		hs.mu.Lock()
		hs.dstats.triedToSend++
		hs.dstats.transcodeFailures++
		hs.dstats.errors[string(b)] = hs.dstats.errors[string(b)] + 1
		hs.mu.Unlock()
		return
	}
	if hs.saveLatencies {
		hs.dstats.latencies = append(hs.dstats.latencies, postTook)
	}
	started := time.Now()
	mediaType, params, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil {
		glog.Error("Error getting mime type ", err, manifestID)
		panic(err)
		return
	}
	var segments [][]byte
	var urls []string
	if "multipart/mixed" == mediaType {
		mr := multipart.NewReader(resp.Body, params["boundary"])
		for {
			p, merr := mr.NextPart()
			if merr == io.EOF {
				break
			}
			if merr != nil {
				glog.Error("Could not process multipart part ", merr, manifestID)
				err = merr
				break
			}
			mediaType, _, err := mime.ParseMediaType(p.Header.Get("Content-Type"))
			if err != nil {
				glog.Error("Error getting mime type ", err, manifestID)
			}
			body, merr := ioutil.ReadAll(p)
			if merr != nil {
				glog.Error("Error reading body ", merr, manifestID)
				err = merr
				break
			}
			if mediaType == "application/vnd+livepeer.uri" {
				urls = append(urls, string(body))

			} else {
				segments = append(segments, body)
			}
		}
	}
	/*
		tbody, err := ioutil.ReadAll(resp.Body)
	*/
	took := time.Since(started)
	glog.Infof("Reading body back for manifest %s took %s profiles %d", manifestID, took, len(segments))
	// glog.Infof("Body: %s", string(tbody))

	if err != nil {
		httpErr := fmt.Sprintf(`Error reading http request body for manifes %s: %s`, manifestID, err.Error())
		glog.Error(httpErr)
		// http.Error(w, httpErr, http.StatusInternalServerError)
		hs.mu.Lock()
		hs.dstats.triedToSend++
		hs.dstats.downloadFailures++
		hs.mu.Unlock()
		return
	}
	resp.Body.Close()
	hs.mu.Lock()
	hs.dstats.triedToSend++
	hs.dstats.success += len(segments)
	hs.mu.Unlock()
	if len(segments) > 0 {
		for i, tseg := range segments {
			hs.mu.Lock()
			hs.dstats.bytes += int64(len(tseg))
			hs.mu.Unlock()
			fsttim, dur, verr := utils.GetVideoStartTimeAndDur(tseg)
			if verr != nil {
				msg := fmt.Sprintf("Error parsing video data (manifest %s) profile %d result status %s video data len %d err %v", manifestID, i,
					resp.Status, len(tseg), verr)
				glog.Error(msg)
				messenger.SendFatalMessage(msg)
				hs.mu.Lock()
				hs.dstats.errors["Video parsing error"] = hs.dstats.errors["Video parsing error"] + 1
				hs.mu.Unlock()
				panic(msg)
			}
			glog.Infof("Got back manifest %s seg seq %d profile %d len %d bytes pts %s dur %s (source duration is %s)", manifestID, seg.seqNo, i, len(tseg), fsttim, dur, seg.duration)
			if savePrefix != "" {
				fn := fmt.Sprintf("trans_%d_%d.ts", i, seg.seqNo)
				err = ioutil.WriteFile(path.Join(savePrefix, fn), tseg, 0644)
				if err != nil {
					glog.Fatal(err)
				}
			}
			if !isTimeEqualM(fsttim, seg.pts) {
				msg := fmt.Sprintf("Manifest %s seg %d source PTS is %s transcoded (profile %d) PTS is %s", manifestID, seg.seqNo, seg.pts, i, fsttim)
				glog.Warning(msg)
				hs.mu.Lock()
				hs.dstats.errors["PTS mismatch"] = hs.dstats.errors["PTS mismatch"] + 1
				hs.mu.Unlock()
			}
			if !isTimeEqualM(dur, seg.duration) {
				msg := fmt.Sprintf("Manifest %s seg %d source duration is %s transcoded (profile %d) duration is %s", manifestID, seg.seqNo, seg.duration, i, dur)
				glog.Warning(msg)
				hs.mu.Lock()
				hs.dstats.errors["Duration mismatch"] = hs.dstats.errors["Duration mismatch"] + 1
				hs.mu.Unlock()
			}
		}
		if savePrefix != "" {
			fn := fmt.Sprintf("source_%d.ts", seg.seqNo)
			err = ioutil.WriteFile(path.Join(savePrefix, fn), seg.data, 0644)
			if err != nil {
				glog.Fatal(err)
			}
		}
	} else if len(urls) > 0 {
		glog.Infof("Manifest %s got %d urls as result:", manifestID, len(urls))
		for _, url := range urls {
			glog.Info(url)
		}
	}
}

func (hs *httpStreamer) closeDone() {
	select {
	case <-hs.done:
	default:
		close(hs.done)
	}
}

func (hs *httpStreamer) stats() httpStats {
	hs.mu.RLock()
	stats := hs.dstats.clone()
	hs.mu.RUnlock()
	return stats
}

func (hs *httpStats) clone() httpStats {
	r := *hs
	for e, i := range hs.errors {
		r.errors[e] = i
	}
	return r
}
