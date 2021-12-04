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
	"strings"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/m3u8"
	"github.com/livepeer/stream-tester/internal/metrics"
	"github.com/livepeer/stream-tester/internal/utils"
	"github.com/livepeer/stream-tester/internal/utils/uhttp"
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
	finite
	baseManifestID string
	saveLatencies  bool
	started        bool
	dstats         httpStats
	mu             sync.RWMutex
	wg             *sync.WaitGroup
}

type httpStats struct {
	triedToSend       int
	sent              int
	failedToSend      int
	downloaded        int
	downloadFailures  int
	transcodeFailures int
	success           int
	success1          int
	bytes             int64
	errors            map[string]int
	latencies         []time.Duration
	finished          bool
	started           time.Time
}

// NewHTTPStreamer ...
func NewHTTPStreamer(pctx context.Context, saveLatencies bool, baseManifestID string) *httpStreamer {
	ctx, cancel := context.WithCancel(pctx)
	hs := &httpStreamer{
		finite: finite{
			ctx:    ctx,
			cancel: cancel,
		},
		saveLatencies:  saveLatencies,
		baseManifestID: baseManifestID,
		wg:             &sync.WaitGroup{},
	}
	hs.dstats.errors = make(map[string]int)
	return hs
}

// var savePrefix = "segmented3"
var savePrefix = ""

func pushHLSSegments(ctx context.Context, manifestFileName string, stopAfter time.Duration, out chan *HlsSegment) error {
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
		return fmt.Errorf("expecting media PL")
	}
	dir := path.Dir(manifestFileName)
	go pushHLSSegmentsLoop(dir, pl, stopAfter, out)
	return nil
}

func pushHLSSegmentsLoop(dir string, pl *m3u8.MediaPlaylist, stopAfter time.Duration, out chan *HlsSegment) {
	for i, seg := range pl.Segments {
		if seg == nil {
			continue
		}
		segData, err := ioutil.ReadFile(path.Join(dir, seg.URI))
		if err != nil {
			out <- &HlsSegment{Err: err}
			return
		}
		fsttim, dur, verr := utils.GetVideoStartTimeAndDur(segData)
		if verr != nil {
			out <- &HlsSegment{Err: verr}
			return
		}
		hseg := &HlsSegment{
			SeqNo:    i,
			Pts:      fsttim,
			Data:     segData,
			Duration: dur,
		}
		time.Sleep(dur)
		out <- hseg
		if stopAfter > 0 && fsttim > stopAfter {
			break
		}
	}
	out <- &HlsSegment{Err: io.EOF}
}

// StartUpload starts HTTP segments. Blocks until end.
func (hs *httpStreamer) StartUpload(fn, httpURL, manifestID string, segmentsToStream int, waitForTarget, stopAfter, skipFirst time.Duration) {
	segmentsIn := make(chan *HlsSegment)
	var err error
	ext := path.Ext(fn)
	if ext == ".m3u8" {
		err = pushHLSSegments(hs.ctx, fn, stopAfter, segmentsIn)
	} else {
		err = StartSegmenting(hs.ctx, fn, true, stopAfter, skipFirst, segLen, true, segmentsIn)
	}
	if err != nil {
		glog.Infof("Error starting segmenter: %v", err)
		panic(err)
	}
	hs.started = true
	metrics.StartStream()
	var seg *HlsSegment
	lastSeg := time.Now()
outloop:
	for {
		select {
		case seg = <-segmentsIn:
		case <-hs.ctx.Done():
			glog.V(model.DEBUG).Infof("=========>>>> got stop singal")
			break outloop
		}
		if seg.Err != nil {
			if seg.Err != io.EOF {
				glog.Warningf("Error during segmenting: %v", seg.Err)
			}
			break
		}
		glog.V(model.VERBOSE).Infof("Got segment out of segmenter mainfest=%s seqNo=%d pts=%s dur=%s since last=%s", manifestID, seg.SeqNo, seg.Pts, seg.Duration, time.Since(lastSeg))
		lastSeg = time.Now()
		hs.wg.Add(1)
		go hs.pushSegment(httpURL, manifestID, seg)
	}
	glog.V(model.VERBOSE).Infof("Waiting for all segments to be pushed mainfest=%s", manifestID)
	hs.wg.Wait()
	hs.mu.Lock()
	hs.dstats.finished = true
	metrics.StopStream(hs.dstats.failedToSend == 0)
	hs.mu.Unlock()
}

func (hs *httpStreamer) pushSegment(httpURL, manifestID string, seg *HlsSegment) {
	defer hs.wg.Done()
	hs.mu.Lock()
	var firstOne bool
	if hs.dstats.started.IsZero() {
		hs.dstats.started = time.Now()
		firstOne = true
	}
	hs.mu.Unlock()
	purl, _ := url.Parse(httpURL)
	host := purl.Hostname()
	urlToUp := fmt.Sprintf("%s/%d.ts", httpURL, seg.SeqNo)
	glog.V(model.DEBUG).Infof("Got source segment manifest=%s seqNo=%d pts=%s dur=%s len=%d bytes from segmenter, uploading to %s", manifestID, seg.SeqNo, seg.Pts, seg.Duration, len(seg.Data), urlToUp)
	var body io.Reader
	body = bytes.NewReader(seg.Data)
	req, err := uhttp.NewRequest("POST", urlToUp, body)
	if err != nil {
		panic(err)
	}
	req.Header.Set("Accept", "multipart/mixed")
	req.Header.Set("Content-Duration", strconv.FormatInt(seg.Duration.Milliseconds(), 10))
	hc := httpClient
	if strings.HasPrefix(urlToUp, "https:") {
		hc = http2Client
	}
	postStarted := time.Now()
	resp, err := hc.Do(req)
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
	glog.V(model.DEBUG).Infof("Post segment manifest=%s seqNo=%d pts=%s dur=%s took=%s timed_out=%v status='%v' err=%v",
		manifestID, seg.SeqNo, seg.Pts, seg.Duration, postTook, timedout, status, err)
	if err != nil {
		hs.mu.Lock()
		hs.dstats.triedToSend++
		hs.dstats.failedToSend++
		if timedout {
			emsg := "Timed out " + host
			hs.dstats.errors[emsg] = hs.dstats.errors[emsg] + 1
		} else if strings.Contains(err.Error(), "write: connection reset by peer") {
			emsg := "connection reset sending source segment to " + host
			hs.dstats.errors[emsg] = hs.dstats.errors[emsg] + 1
		} else if strings.Contains(err.Error(), "read: connection reset by peer") {
			emsg := "connection reset reading status from " + host
			hs.dstats.errors[emsg] = hs.dstats.errors[emsg] + 1
		} else if strings.Contains(err.Error(), "server misbehaving") {
			emsg := "DNS server misbehaving for " + host
			hs.dstats.errors[emsg] = hs.dstats.errors[emsg] + 1
		} else if strings.Contains(err.Error(), "http2: stream closed") {
			emsg := "http2: stream closed posting to " + host
			hs.dstats.errors[emsg] = hs.dstats.errors[emsg] + 1
		} else {
			hs.dstats.errors[err.Error()] = hs.dstats.errors[err.Error()] + 1
		}
		hs.mu.Unlock()
		return
		// panic(err)
	}
	glog.V(model.VERBOSE).Infof("Got transcoded manifest=%s seqNo=%d resp status=%s reading body started", manifestID, seg.SeqNo, resp.Status)
	if resp.StatusCode != http.StatusOK {
		b, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		hs.mu.Lock()
		hs.dstats.triedToSend++
		hs.dstats.transcodeFailures++
		hs.dstats.errors[string(b)] = hs.dstats.errors[string(b)] + 1
		statusStr := fmt.Sprintf("Pushing segment status error %d (%s) (%s)", resp.StatusCode, host, b)
		hs.dstats.errors[statusStr] = hs.dstats.errors[statusStr] + 1
		hs.dstats.errors[string(b)] = hs.dstats.errors[string(b)] + 1
		hs.mu.Unlock()
		glog.V(model.DEBUG).Infof("Got manifest=%s seqNo=%d resp status=%s error in body $%s", manifestID, seg.SeqNo, resp.Status, string(b))
		return
	}
	if firstOne {
		metrics.StartupLatency(postTook)
	}
	metrics.TranscodeLatency(postTook)
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
		// return
	}
	glog.V(model.VERBOSE).Infof("mediaType=%s params=%+v", mediaType, params)
	if glog.V(model.VVERBOSE) {
		for k, v := range resp.Header {
			glog.Infof("Header '%s': '%s'", k, v)
		}
	}
	var segments [][]byte
	var urls []string
	if mediaType == "multipart/mixed" {
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
				for k, v := range p.Header {
					glog.Infof("Header '%s': '%s'", k, v)
				}
			}
			body, merr := ioutil.ReadAll(p)
			if merr != nil {
				glog.Errorf("error reading body manifest=%s seqNo=%d err=%v", manifestID, seg.SeqNo, merr)
				err = merr
				break
			}
			if mediaType == "application/vnd+livepeer.uri" {
				urls = append(urls, string(body))
			} else {
				var v glog.Level = model.DEBUG
				if len(body) < 5 {
					v = 0
				}
				glog.V(v).Infof("Read back segment for manifest=%s seqNo=%d profile=%d len=%d bytes", manifestID, seg.SeqNo, len(segments), len(body))
				segments = append(segments, body)
			}
		}
	}
	/*
		tbody, err := ioutil.ReadAll(resp.Body)
	*/
	took := time.Since(started)
	glog.V(model.VERBOSE).Infof("Reading body back for manifest=%s seqNo=%d took=%s profiles=%d", manifestID, seg.SeqNo, took, len(segments))
	// glog.Infof("Body: %s", string(tbody))

	if err != nil {
		httpErr := fmt.Sprintf(`Error reading http request body for manifest=%s seqNo=%d err=%s`, manifestID, seg.SeqNo, err.Error())
		glog.Error(httpErr)
		// http.Error(w, httpErr, http.StatusInternalServerError)
		hs.mu.Lock()
		hs.dstats.triedToSend++
		hs.dstats.downloadFailures++
		if strings.Contains(err.Error(), "Client.Timeout exceeded while reading body") {
			errt := "timed out downloading transcoded data " + host
			hs.dstats.errors[errt] = hs.dstats.errors[errt] + 1
		} else if strings.Contains(err.Error(), "connection reset by peer") {
			errt := "connection reset downloading transcoded data from " + host
			hs.dstats.errors[errt] = hs.dstats.errors[errt] + 1
		} else {
			hs.dstats.errors[err.Error()] = hs.dstats.errors[err.Error()] + 1
		}
		hs.mu.Unlock()
		return
	}
	resp.Body.Close()
	hs.mu.Lock()
	hs.dstats.triedToSend++
	hs.dstats.success += len(segments)
	hs.dstats.success1++
	hs.mu.Unlock()
	if len(segments) > 0 {
		var panicerr error
		for i, tseg := range segments {
			hs.mu.Lock()
			hs.dstats.bytes += int64(len(tseg))
			hs.mu.Unlock()
			fsttim, dur, verr := utils.GetVideoStartTimeAndDur(tseg)
			if verr != nil {
				msg := fmt.Sprintf("Error parsing video data (manifest=%s seqNo=%d) profile %d result status=%s video data len=%d err=%v",
					manifestID, seg.SeqNo, i, resp.Status, len(tseg), verr)
				glog.Error(msg)
				messenger.SendFatalMessage(msg)
				hs.mu.Lock()
				hs.dstats.errors["Video parsing error"] = hs.dstats.errors["Video parsing error"] + 1
				hs.mu.Unlock()
				if model.FailHardOnBadSegments {
					fname := fmt.Sprintf("bad_video_%s_%d_%d.ts", manifestID, seg.SeqNo, i)
					ioutil.WriteFile(fname, tseg, 0644)
					srcfname := fmt.Sprintf("bad_video_%s_%d_source.ts", manifestID, seg.SeqNo)
					ioutil.WriteFile(srcfname, seg.Data, 0644)
					glog.Infof("Wrote bad segment to '%s', source segment to '%s'", fname, srcfname)
					// glog.Infof("Data:\n%x", tseg)
					// glog.Infof("Data as string:\n%s", string(tseg))
					// panic(msg)
					panicerr = verr
				}
			}
			glog.V(model.VERBOSE).Infof("Got back manifest=%s seg seqNo=%d profile=%d len=%d bytes pts=%s dur=%s (source duration is %s)",
				manifestID, seg.SeqNo, i, len(tseg), fsttim, dur, seg.Duration)
			if savePrefix != "" {
				fn := fmt.Sprintf("trans_%d_%d.ts", i, seg.SeqNo)
				err = ioutil.WriteFile(path.Join(savePrefix, fn), tseg, 0644)
				if err != nil {
					glog.Fatal(err)
				}
			}
			if !isTimeEqualM(fsttim, seg.Pts) {
				msg := fmt.Sprintf("Manifest=%s seqNo=%d source pts=%s transcoded (profile=%d) pts=%s", manifestID, seg.SeqNo, seg.Pts, i, fsttim)
				glog.Warning(msg)
				hs.mu.Lock()
				hs.dstats.errors["PTS mismatch"] = hs.dstats.errors["PTS mismatch"] + 1
				hs.mu.Unlock()
			}
			if !isTimeEqualM(dur, seg.Duration) {
				msg := fmt.Sprintf("Manifest=%s seqNo=%d source duration=%s transcoded (profile=%d) duration=%s", manifestID, seg.SeqNo, seg.Duration, i, dur)
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
			fn := fmt.Sprintf("source_%d.ts", seg.SeqNo)
			err = ioutil.WriteFile(path.Join(savePrefix, fn), seg.Data, 0644)
			if err != nil {
				glog.Fatal(err)
			}
		}
	} else if len(urls) > 0 {
		glog.Infof("Manifest=%s seqNo=%d got %d urls as result: %+v", manifestID, seg.SeqNo, len(urls), urls)
	}
}

func (hs *httpStreamer) StatsOld() (*model.Stats, error) {
	s := hs.stats()
	return s.Stats()
}

func (hs *httpStreamer) Stats() (model.Stats1, error) {
	s := hs.stats()
	stats, _ := s.Stats()
	glog.V(model.VERBOSE).Infof("====> stats is\n===> %+v", stats)
	stats1 := model.Stats1{
		SuccessRate:         stats.SuccessRate / 100.0,
		Finished:            stats.Finished,
		Started:             hs.started,
		SourceLatencies:     stats.SourceLatencies,
		TranscodedLatencies: stats.TranscodedLatencies,
	}
	return stats1, nil
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

func (hs *httpStats) Stats() (*model.Stats, error) {
	stats := &model.Stats{
		RTMPstreams:         0,
		MediaStreams:        1,
		TotalSegmentsToSend: 0,
		Finished:            true,
	}
	transcodedLatencies := utils.LatenciesCalculator{}
	if stats.StartTime.IsZero() {
		stats.StartTime = hs.started
	} else if !hs.started.IsZero() && stats.StartTime.After(hs.started) {
		stats.StartTime = hs.started
	}
	stats.SentSegments += hs.triedToSend
	stats.DownloadedSegments += hs.success
	stats.FailedToDownloadSegments += hs.downloadFailures
	stats.BytesDownloaded += hs.bytes
	transcodedLatencies.Add(hs.latencies)
	if hs.errors != nil {
		if stats.Errors == nil {
			stats.Errors = make(map[string]int)
		}
		for e, i := range hs.errors {
			stats.Errors[e] = i
		}
	}
	if !hs.finished {
		stats.Finished = false
	}
	transcodedLatencies.Prepare()
	avg, p50, p95, p99 := transcodedLatencies.Calc()
	stats.TranscodedLatencies = model.Latencies{Avg: avg, P50: p50, P95: p95, P99: p99}
	if stats.SentSegments > 0 {
		// stats.SuccessRate = float64(stats.DownloadedSegments) / (float64(model.ProfilesNum) * float64(stats.SentSegments)) * 100
		stats.SuccessRate = float64(hs.success1) / float64(hs.triedToSend) * 100
	}
	stats.ShouldHaveDownloadedSegments = model.ProfilesNum * stats.SentSegments
	stats.ProfilesNum = model.ProfilesNum
	stats.RawTranscodedLatencies = transcodedLatencies.Raw()
	return stats, nil
}
