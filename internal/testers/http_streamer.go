package testers

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/m3u8"
	"github.com/livepeer/stream-tester/internal/utils"
	"github.com/livepeer/stream-tester/messenger"
	"github.com/livepeer/stream-tester/model"
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
	ctx            context.Context
	baseManifestID string
	saveLatencies  bool
	dstats         httpStats
	mu             sync.RWMutex
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
	finished          bool
	started           time.Time
}

func newHTTPtreamer(ctx context.Context, saveLatencies bool, baseManifestID string) *httpStreamer {
	hs := &httpStreamer{ctx: ctx, saveLatencies: saveLatencies, baseManifestID: baseManifestID}
	hs.dstats.errors = make(map[string]int)
	return hs
}

func (hs *httpStreamer) Stop() {

}

// var savePrefix = "segmented3"
var savePrefix = ""

func pushHLSSegments(ctx context.Context, manifestFileName string, stopAfter time.Duration, out chan *hlsSegment) error {
	f, err := os.Open(manifestFileName)
	if err != nil {
		return err
	}
	p, _, err := m3u8.DecodeFrom(bufio.NewReader(f), true)
	if err != nil {
		return err
	}
	f.Close()
	pl, ok := p.(*m3u8.MediaPlaylist)
	if !ok {
		return fmt.Errorf("Expecting media PL")
	}
	dir := path.Dir(manifestFileName)
	go pushHLSSegmentsLoop(dir, pl, stopAfter, out)
	return nil
}

func pushHLSSegmentsLoop(dir string, pl *m3u8.MediaPlaylist, stopAfter time.Duration, out chan *hlsSegment) {
	for i, seg := range pl.Segments {
		if seg == nil {
			continue
		}
		segData, err := ioutil.ReadFile(path.Join(dir, seg.URI))
		if err != nil {
			out <- &hlsSegment{err: err}
			return
		}
		fsttim, dur, verr := utils.GetVideoStartTimeAndDur(segData)
		if verr != nil {
			out <- &hlsSegment{err: verr}
			return
		}
		hseg := &hlsSegment{
			seqNo:    i,
			pts:      fsttim,
			data:     segData,
			duration: dur,
		}
		time.Sleep(dur)
		out <- hseg
		if stopAfter > 0 && fsttim > stopAfter {
			break
		}
	}
	out <- &hlsSegment{err: io.EOF}
}

// StartUpload starts HTTP segments. Blocks until end.
func (hs *httpStreamer) StartUpload(fn, httpURL, manifestID string, segmentsToStream int, waitForTarget, stopAfter time.Duration) {
	segmentsIn := make(chan *hlsSegment)
	var err error
	ext := path.Ext(fn)
	if ext == ".m3u8" {
		err = pushHLSSegments(hs.ctx, fn, stopAfter, segmentsIn)
	} else {
		err = startSegmenting(hs.ctx, fn, true, stopAfter, segmentsIn)
	}
	if err != nil {
		glog.Infof("Error starting segmenter: %v", err)
		panic(err)
	}
	var seg *hlsSegment
outloop:
	for {
		select {
		case seg = <-segmentsIn:
		case <-hs.ctx.Done():
			glog.Infof("=========>>>> got stop singal")
			break outloop
		}
		if seg.err != nil {
			if seg.err != io.EOF {
				glog.Warningf("Error during segmenting: %v", seg.err)
			}
			break
		}
		go hs.pushSegment(httpURL, manifestID, seg)
	}
	hs.mu.Lock()
	hs.dstats.finished = true
	hs.mu.Unlock()
}

func (hs *httpStreamer) pushSegment(httpURL, manifestID string, seg *hlsSegment) {
	hs.mu.Lock()
	if hs.dstats.started.IsZero() {
		hs.dstats.started = time.Now()
	}
	hs.mu.Unlock()
	urlToUp := fmt.Sprintf("%s/%d.ts", httpURL, seg.seqNo)
	glog.V(model.SHORT).Infof("Got segment manifest %s seqNo %d pts %s dur %s bytes %d from segmenter, uploading to %s", manifestID, seg.seqNo, seg.pts, seg.duration, len(seg.data), urlToUp)
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
	glog.V(model.DEBUG).Infof("Post segment manifest %s seqNo %d pts %s dur %s took %s timed out %v status '%v'", manifestID, seg.seqNo, seg.pts, seg.duration, postTook, timedout, status)
	if err != nil {
		hs.mu.Lock()
		hs.dstats.triedToSend++
		hs.dstats.failedToSend++
		hs.mu.Unlock()
		return
		// panic(err)
	}
	glog.V(model.VERBOSE).Infof("Got manifest %s resp status %s reading body started", manifestID, resp.Status)
	if resp.StatusCode != http.StatusOK {
		b, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		hs.mu.Lock()
		hs.dstats.triedToSend++
		hs.dstats.transcodeFailures++
		hs.dstats.errors[string(b)] = hs.dstats.errors[string(b)] + 1
		hs.mu.Unlock()
		glog.V(model.DEBUG).Infof("Got manifest %s resp status %s error in body $%s", manifestID, resp.Status, string(b))
		return
	}
	if hs.saveLatencies {
		hs.mu.Lock()
		hs.dstats.latencies = append(hs.dstats.latencies, postTook)
		hs.mu.Unlock()
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
	glog.V(model.VERBOSE).Infof("Reading body back for manifest %s took %s profiles %d", manifestID, took, len(segments))
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
		var panicerr error
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
				if model.FailHardOnBadSegments {
					fname := fmt.Sprintf("bad_video_%s_%d_%d.ts", manifestID, seg.seqNo, i)
					ioutil.WriteFile(fname, tseg, 0644)
					srcfname := fmt.Sprintf("bad_video_%s_%d_source.ts", manifestID, seg.seqNo)
					ioutil.WriteFile(srcfname, seg.data, 0644)
					glog.Infof("Wrote bad segment to '%s', source segment to '%s'", fname, srcfname)
					// glog.Infof("Data:\n%x", tseg)
					// glog.Infof("Data as string:\n%s", string(tseg))
					// panic(msg)
					panicerr = verr
				}
			}
			glog.V(model.VERBOSE).Infof("Got back manifest %s seg seq %d profile %d len %d bytes pts %s dur %s (source duration is %s)", manifestID, seg.seqNo, i, len(tseg), fsttim, dur, seg.duration)
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
		if panicerr != nil {
			panic(panicerr)
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

func (hs *httpStreamer) stats() httpStats {
	hs.mu.RLock()
	stats := hs.dstats.clone()
	hs.mu.RUnlock()
	return stats
}

func (hs *httpStats) clone() httpStats {
	r := *hs
	r.errors = make(map[string]int)
	for e, i := range hs.errors {
		r.errors[e] = i
	}
	if len(hs.latencies) > 0 {
		r.latencies = make([]time.Duration, len(hs.latencies))
		copy(r.latencies, hs.latencies)
	}
	return r
}
