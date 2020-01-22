package testers

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strconv"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/joy4/jerrors"
	"github.com/livepeer/m3u8"
	"github.com/livepeer/stream-tester/messenger"
	"github.com/livepeer/stream-tester/model"
	"github.com/livepeer/stream-tester/internal/utils"
)

// IgnoreGaps ...
var IgnoreGaps bool

// IgnoreTimeDrift ...
var IgnoreTimeDrift bool

const (
	// reportStatsEvery = 5 * time.Minute
	// reportStatsEvery = 30 * time.Second
	reportStatsEvery  = 4 * 60 * time.Second
	reportStatsEvery2 = 30 * time.Minute
)

type (
	resolution string

	finite struct {
		done chan struct{} // signals to stop
	}
	// m3utester2 tests one stream, reading all the media streams
	m3utester2 struct {
		finite
		initialURL      *url.URL
		wowzaMode       bool
		streams         map[resolution]*m3uMediaStream
		downloadResults chan *downloadResult
		latencyResults  chan *latencyResult
		segmentsMatcher *segmentsMatcher
	}

	// m3uMediaStream downloads media stream. Hadle stream changes
	// (must be notified about new url by m3utester2)
	m3uMediaStream struct {
		finite
		name             string // usually medial playlist relative name
		resolution       string
		u                *url.URL
		downloadResults  chan *downloadResult
		streamSwitchedTo chan nameAndURI
		segmentsMatcher  *segmentsMatcher
	}

	nameAndURI struct {
		name string
		uri  *url.URL
	}

	latencyResult struct {
		resolution string
		seqNo      uint64
		name       string
		latency    time.Duration
		speedRatio float64
	}
)

func newM3utester2(u string, wowzaMode bool, done chan struct{}, waitForTarget time.Duration, sm *segmentsMatcher) *m3utester2 {
	iu, err := url.Parse(u)
	if err != nil {
		glog.Fatal(err)
	}
	mut := &m3utester2{
		finite: finite{
			done: done,
		},
		initialURL:      iu,
		wowzaMode:       wowzaMode,
		streams:         make(map[resolution]*m3uMediaStream),
		downloadResults: make(chan *downloadResult, 32),
		latencyResults:  make(chan *latencyResult, 32),
		segmentsMatcher: sm,
	}
	go mut.workerLoop()
	go mut.manifestPullerLoop(waitForTarget)
	return mut
}

func newM3uMediaStream(name, resolution string, u *url.URL, wowzaMode bool, done chan struct{}, masterDR chan *downloadResult, sm *segmentsMatcher,
	latencyResults chan *latencyResult) *m3uMediaStream {

	ms := &m3uMediaStream{
		finite: finite{
			done: done,
		},
		name:             name,
		u:                u,
		resolution:       resolution,
		downloadResults:  make(chan *downloadResult, 32),
		streamSwitchedTo: make(chan nameAndURI),
		segmentsMatcher:  sm,
	}
	go ms.workerLoop(masterDR, latencyResults)
	go ms.manifestPullerLoop(wowzaMode)
	return ms
}

func (mut *m3utester2) workerLoop() {
	seenResolutions := make(sort.StringSlice, 0, 16)
	results := make(map[string]downloadResultsByAtFirstTime)
	started := time.Now()
	timer := time.NewTimer(reportStatsEvery)
	// transcodedLatencies := utils.LatenciesCalculator{}
	latencies := make(map[string]*utils.DurationsCapped)
	printStats := func() {
		msg := fmt.Sprintf("Stream %s is running for %s already\n", mut.initialURL, time.Since(started))
		if len(latencies) > 0 {
			msg += "```"
			for res, lat := range latencies {
				clat := lat.GetPercentile(50, 95, 99)
				flat := lat.GetPercentileFloat(50, 95, 99)
				msg += fmt.Sprintf("Latencies    for %12s is p50 %s p95 %s p99 %s\n", res, clat[0], clat[1], clat[2])
				msg += fmt.Sprintf("Speed ratios for %12s is p50 %v p95 %v p99 %v\n", res, flat[0], flat[1], flat[2])
			}
			msg += "```"
		}
		messenger.SendMessage(msg)
	}
	problems := 0
	var lastTimeDriftReportTime time.Time
	for {
		select {
		case <-mut.done:
			printStats()
			return
		case <-timer.C:
			printStats()
			if time.Since(started) > time.Hour {
				timer.Reset(reportStatsEvery2)
			} else {
				timer.Reset(reportStatsEvery)
			}
		case lres := <-mut.latencyResults:
			if _, has := latencies[lres.resolution]; !has {
				latencies[lres.resolution] = utils.NewDurations(256)
			}
			latencies[lres.resolution].Add(lres.latency)
			latencies[lres.resolution].AddFloat(lres.speedRatio)

		case dres := <-mut.downloadResults:
			// glog.Infof("MAIN downloaded: %s", dres.String2())
			// continue
			if _, has := results[dres.resolution]; !has {
				results[dres.resolution] = make(downloadResultsByAtFirstTime, 0, 128)
				seenResolutions = append(seenResolutions, dres.resolution)
				seenResolutions.Sort()
			}
			results[dres.resolution] = append(results[dres.resolution], dres)
			sort.Sort(results[dres.resolution])
			if len(results[dres.resolution]) > 128 {
				results[dres.resolution] = results[dres.resolution][1:]
			}
			// check for time drift
			// if dres.seqNo < 24 {
			// 	continue
			// }
			if len(seenResolutions) < 2 {
				continue
			}
			// find corresponding segments
			correspondingSegments := make(map[string]*downloadResult)
			correspondingSegments[dres.resolution] = dres
			for ri := 0; ri < len(seenResolutions); ri++ {
				resolution := seenResolutions[ri]
				if resolution == dres.resolution {
					continue
				}
				segments := results[resolution]
				for si := len(segments) - 1; si >= 0; si-- {
					seg := segments[si]
					if isTimeEqualTD(dres.timeAtFirstPlace, seg.timeAtFirstPlace, time.Second) {
						correspondingSegments[seg.resolution] = seg
						break
					}
				}
			}
			// debug print
			/*
				if len(correspondingSegments) > 1 {
					glog.Infof("=======>>> Corresponding segments for %s (%d segs)", dres.String(), len(correspondingSegments))
					for _, res := range seenResolutions {
						if seg, has := correspondingSegments[res]; has {
							// glog.Infof("=====> %s", seg.String2())
							fmt.Printf("=====> %s\n", seg.String2())
						}
					}
				}
					glog.Infof("=======>>> segments by time at first ")
					for _, res := range seenResolutions {
						for _, seg := range results[res] {
							// glog.Infof("=====> %s", seg.String2())
							fmt.Printf("=====> %s\n", seg.String2())
						}
					}
			*/
			// check for drift
			// glog.Infof("=====> seen resolutions: %+v", seenResolutions)
			for i := 0; i < len(seenResolutions)-1; i++ {
				for j := i + 1; j < len(seenResolutions); j++ {
					res1 := seenResolutions[i]
					res2 := seenResolutions[j]
					// glog.Infof("===> i %d j %d res1 %s res2 %s", i, j, res1, res2)
					seg1, has1 := correspondingSegments[res1]
					seg2, has2 := correspondingSegments[res2]
					if has1 && has2 {
						if diff := absTimeTiff(seg1.startTime, seg2.startTime); diff > 4*time.Second {
							msg := fmt.Sprintf("Too big (%s) time difference between %s stream (time %s seqNo %d) and %s stream (time %s seqNo %d)",
								diff, res1, seg1.startTime, seg1.seqNo, res2, seg2.startTime, seg2.seqNo)
							// glog.Info("================#########>>>>>>")
							// glog.Info(msg)
							if !IgnoreTimeDrift {
								mut.fatalEnd(msg)
								return
							}
							if time.Since(lastTimeDriftReportTime) > 5*time.Minute {
								glog.Info(msg)
								messenger.SendMessage(msg)
								lastTimeDriftReportTime = time.Now()
							}
							// if problems > 100 {
							// 	panic(msg)
							// }
							problems++
						}
					}
				}
			}
			/*
				for ri := 0; ri <= len(seenResolutions); ri++ {
					// if resolution == dres.resolution {
					// 	continue
					// }

					resolution1 := seenResolutions[i]
					// find corresponding segment
					for i := len(ress) - 1; i >= 0; i-- {
						cseg := ress[i]
						if isTimeEqualT(dres.appTime, cseg.appTime) {
							glog.Infof("=======>>> Corresponding segments:\n%s\n%s\n", dres.String(), cseg.String())
							diff := absTimeTiff(dres.startTime, cseg.startTime)
							if diff > 2*time.Second {
								// msg := fmt.Sprintf("\nTime drift detected: %s : %s\ndiff: %d", dres.String(), cseg.String(), diff)
								// mut.fatalEnd(msg)
							}
						}
						break
					}
				}
			*/
		}
	}
}

func (f *finite) fatalEnd(msg string) {
	glog.Error(msg)
	messenger.SendFatalMessage(msg)
	select {
	case <-f.done:
	default:
		close(f.done)
	}
	// panic(msg)
}

// continiously pull main manifest, starts new media streams
// or notifies existing media stream about stream change
func (mut *m3utester2) manifestPullerLoop(waitForTarget time.Duration) {
	surl := mut.initialURL.String()
	startedAt := time.Now()
	var gotManifest, streamStarted bool
	var lastNumberOfStreamsInManifest int = -1

	countTimeouts := 0
	for {
		select {
		case <-mut.done:
			return
		default:
		}
		if waitForTarget > 0 && !gotManifest && time.Since(startedAt) > waitForTarget {
			mut.fatalEnd(fmt.Sprintf("Can't get playlist %s for %s, giving up.", surl, time.Since(startedAt)))
			return
		}
		resp, err := httpClient.Get(surl)
		if err != nil {
			uerr := err.(*url.Error)
			if uerr.Timeout() {
				countTimeouts++
				if countTimeouts > 32 {
					mut.fatalEnd(fmt.Sprintf("Fatal timeout error trying to get playlist %s: %v", surl, err))
					return
				}
				time.Sleep(2 * time.Second)
				continue
			}
			mut.fatalEnd(fmt.Sprintf("Fatal error trying to get playlist %s: %v", surl, err))
			return
		}
		countTimeouts = 0
		if resp.StatusCode != http.StatusOK {
			b, _ := ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			glog.Infof("===== status error getting master playlist %s: %v (%s) body: %s", surl, resp.StatusCode, resp.Status, string(b))
			time.Sleep(2 * time.Second)
			continue
		}
		gotManifest = true
		glog.V(model.DEBUG).Infof("Main manifest status %v", resp.Status)
		// glog.Infof("Main manifest status %v", resp.Status)
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			glog.Info("===== error getting master playlist: ", err)
			// glog.Error(err)
			time.Sleep(2 * time.Second)
			continue
		}
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
		glog.V(model.VERBOSE).Infof("Got playlist with %d variants (%s):", len(mpl.Variants), surl)
		glog.V(model.VERBOSE).Info(mpl)
		if lastNumberOfStreamsInManifest != len(mpl.Variants) {
			glog.Infof("Got playlist with %d variants (%s):", len(mpl.Variants), surl)
		}
		// glog.Infof("==> model.ProfilesNum = %d", model.ProfilesNum)
		if !streamStarted && len(mpl.Variants) >= model.ProfilesNum+1 {
			streamStarted = true
		} else if !streamStarted && time.Since(startedAt) > 2*waitForTarget {
			msg := fmt.Sprintf("Stream started %s ago, but main manifest still has %d profiles instead of %d. Stopping.",
				time.Since(startedAt), len(mpl.Variants), model.ProfilesNum+1)
			glog.Error(msg)
			mut.fatalEnd(msg)
			return
		}
		lastNumberOfStreamsInManifest = len(mpl.Variants)
		// glog.Infof("Got playlist with %d variants (%s):", len(mpl.Variants), surl)
		// glog.Info(mpl)
		seenResolution := newStringRing(len(mpl.Variants))
		for _, variant := range mpl.Variants {
			// glog.Infof("Variant URI: %s", variant.URI)
			if variant.Resolution == "" {
				msg := fmt.Sprintf("Got variant %s without resolution, stopping", variant.URI)
				mut.fatalEnd(msg)
				return
			}
			if seenResolution.Contains(variant.Resolution) {
				msg := fmt.Sprintf("Playlist has resolution %s more than once, stopping", variant.Resolution)
				mut.fatalEnd(msg)
				return
			}
			seenResolution.Add(variant.Resolution)
			res := resolution(variant.Resolution)

			if mut.wowzaMode {
				// remove Wowza's session id from URL
				variant.URI = wowzaSessionRE.ReplaceAllString(variant.URI, "_")
			}
			pvrui, err := url.Parse(variant.URI)
			if err != nil {
				msg := fmt.Sprintf("Erorr parsing variant url %s: %v", variant.URI, err)
				mut.fatalEnd(msg)
				return
			}
			// glog.Infof("Parsed uri: %+v", pvrui, pvrui.IsAbs)
			if !pvrui.IsAbs() {
				pvrui = mut.initialURL.ResolveReference(pvrui)
			}
			if ms, has := mut.streams[res]; has {
				if ms.name != variant.URI {
					// stream changed, notify stream downloaded
					ms.streamSwitchedTo <- nameAndURI{name: variant.URI, uri: pvrui} // blocks until processed, needed to make sure name was changed
				}
				continue
			}

			glog.Info(pvrui)
			stream := newM3uMediaStream(variant.URI, variant.Resolution, pvrui, mut.wowzaMode, mut.done, mut.downloadResults, mut.segmentsMatcher, mut.latencyResults)
			mut.streams[res] = stream
		}
		time.Sleep(1 * time.Second)
	}
}

type downloadResultsByAppTime []*downloadResult

func (p downloadResultsByAppTime) Len() int { return len(p) }
func (p downloadResultsByAppTime) Less(i, j int) bool {
	return p[i].appTime.Before(p[j].appTime)
}
func (p downloadResultsByAppTime) Swap(i, j int) { p[i], p[j] = p[j], p[i] }

type downloadResultsByAtFirstTime []*downloadResult

func (p downloadResultsByAtFirstTime) Len() int { return len(p) }
func (p downloadResultsByAtFirstTime) Less(i, j int) bool {
	return p[i].timeAtFirstPlace.Before(p[j].timeAtFirstPlace)
}
func (p downloadResultsByAtFirstTime) Swap(i, j int) { p[i], p[j] = p[j], p[i] }

func (ms *m3uMediaStream) workerLoop(masterDR chan *downloadResult, latencyResults chan *latencyResult) {
	results := make(downloadResultsByAppTime, 0, 128)
	var lastPrintTime time.Time
	for {
		select {
		case <-ms.done:
			return
		case dres := <-ms.downloadResults:
			// glog.Infof("== ms loop downloaded status %s res %s name %s seqNo %d len %d", dres.status, dres.resolution, dres.name, dres.seqNo, dres.bytes)
			if dres.status != "200 OK" {
				continue
			}
			if !dres.timeAtFirstPlace.IsZero() {
				for i := len(results) - 1; i >= 0; i-- {
					r := results[i]
					if r.seqNo == dres.seqNo && r.name == r.name {
						r.timeAtFirstPlace = dres.timeAtFirstPlace
						masterDR <- r
						break
					}
				}
				continue
			}
			dres.resolution = ms.resolution
			// glog.Infof("downloaded: %+v", dres)
			if dres.videoParseError != nil {
				msg := fmt.Sprintf("Error parsing video segment: %v", dres.videoParseError)
				if IgnoreNoCodecError && (errors.Is(dres.videoParseError, jerrors.ErrNoAudioInfoFound) || errors.Is(dres.videoParseError, jerrors.ErrNoVideoInfoFound)) {
					messenger.SendFatalMessage(msg)
					continue
				} else {
					ms.fatalEnd(msg)
					return
				}
			}
			var latency time.Duration
			var speedRatio float64
			var merr error
			if ms.segmentsMatcher != nil {
				latency, speedRatio, merr = ms.segmentsMatcher.matchSegment(dres.startTime, dres.duration, dres.downloadCompetedAt)
			}
			glog.Infof(`%s seqNo %4d latency is %s speedRatio is %v`, dres.resolution, dres.seqNo, latency, speedRatio)
			if merr != nil {
				glog.Infof("downloaded: %+v", dres)
				messenger.SendFatalMessage(merr.Error())
				panic(merr)
				continue
			}
			if ms.segmentsMatcher != nil {
				latencyResults <- &latencyResult{name: dres.name, resolution: ms.resolution, seqNo: dres.seqNo, latency: latency, speedRatio: speedRatio}
			}
			// masterDR <- dres
			results = append(results, dres)
			sort.Sort(results)
			if len(results) > 128 {
				results = results[1:]
			}
			// check for gaps
			var tillNext time.Duration
			var problem, fatalProblem string
			var print bool
			res := ""
			for i, r := range results {
				problem = ""
				tillNext = 0
				if i < len(results)-2 {
					ns := results[i+1]
					tillNext = ns.startTime - r.startTime
					if tillNext > 0 && !isTimeEqualM(r.duration, tillNext) {
						problem = fmt.Sprintf(" ===> possible gap - to big time difference %s (d2 i: %d, now %d)", tillNext-r.duration, i, time.Now().UnixNano())
						print = true
					}
				}
				msg := fmt.Sprintf("%10s %14s seq %3d: time %s duration %s till next %s appearance time %s %s\n",
					ms.resolution, r.name, r.seqNo, r.startTime, r.duration, tillNext, r.appTime, problem)
				res += msg
				// if problem != "" && i < len(results)-6 {
				// 	fmt.Println(res)
				// 	ms.fatalEnd(msg)
				// 	return
				// }
				if problem != "" && i < len(results)-6 {
					fatalProblem = msg
					// break
				}
			}
			if print && time.Since(lastPrintTime) > 10*time.Second || fatalProblem != "" && !IgnoreGaps {
				fmt.Println(res)
				lastPrintTime = time.Now()
			}
			if fatalProblem != "" {
				if !IgnoreGaps {
					ms.fatalEnd(fatalProblem)
					return
				}
				messenger.SendMessageDebounced(fatalProblem)
			}
		}
	}
}

// continiously pull main manifest, starts new media streams
// or notifies existing media stream about stream change
func (ms *m3uMediaStream) manifestPullerLoop(wowzaMode bool) {
	surl := ms.u.String()
	// startedAt := time.Now()
	seen := newStringRing(128)
	seenAtFirst := newStringRing(128)
	lastTimeNewSegmentSeen := time.Now()
	countTimeouts := 0
	for {
		select {
		case <-ms.done:
			return
		case switched := <-ms.streamSwitchedTo:
			glog.Infof(`Stream %s (%s) switched to %s (%s)`, ms.name, surl, switched.name, switched.uri.String())
			ms.name = switched.name
			ms.u = switched.uri
			surl = switched.uri.String()
			lastTimeNewSegmentSeen = time.Now()
		default:
		}
		if time.Since(lastTimeNewSegmentSeen) > 32*time.Second {
			msg := fmt.Sprintf("Stream %s not seen new segments for %s, stopping.", surl, time.Since(lastTimeNewSegmentSeen))
			ms.fatalEnd(msg)
			return
		}
		resp, err := httpClient.Get(surl)
		if err != nil {
			uerr := err.(*url.Error)
			if uerr.Timeout() {
				countTimeouts++
				if countTimeouts > 15 {
					ms.fatalEnd(fmt.Sprintf("Fatal timeout error trying to get media playlist %s: %v", surl, err))
					return
				}
				time.Sleep(2 * time.Second)
				continue
			}
			ms.fatalEnd(fmt.Sprintf("Fatal error trying to get media playlist %s: %v", surl, err))
			return
		}
		countTimeouts = 0
		if resp.StatusCode != http.StatusOK {
			b, _ := ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			glog.Infof("===== status error getting media playlist %s: %v (%s) body: %s", surl, resp.StatusCode, resp.Status, string(b))
			time.Sleep(2 * time.Second)
			continue
		}
		b, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			ms.fatalEnd(fmt.Sprintf("Fatal error trying to read media playlist %s: %v", surl, err))
			return
		}
		pl, err := m3u8.NewMediaPlaylist(100, 100)
		if err != nil {
			glog.Fatal(err)
		}
		err = pl.Decode(*bytes.NewBuffer(b), true)
		if err != nil {
			glog.Fatal(err)
		}
		glog.V(model.VERBOSE).Infof("Got media playlist %s with %d (really %d) segments of url %s:", ms.resolution, len(pl.Segments), countSegments(pl), surl)
		glog.V(model.VERBOSE).Info(pl)

		// glog.Infof("Got media playlist %s with %d (really %d) segments of url %s:", ms.resolution, len(pl.Segments), countSegments(pl), surl)
		// glog.Info(pl)
		now := time.Now()
		var lastTimeDownloadStarted time.Time
		for i, segment := range pl.Segments {
			if segment != nil {
				// glog.Infof("Segment: %+v", *segment)
				if wowzaMode {
					// remove Wowza's session id from URL
					segment.URI = wowzaSessionRE.ReplaceAllString(segment.URI, "_")
				}
				if i == 0 && !seenAtFirst.Contains(segment.URI) && seen.Contains(segment.URI) {
					glog.V(model.DEBUG).Infof("===> segment at first place %s (%s) seq %d", segment.URI, ms.resolution, pl.SeqNo)
					ms.downloadResults <- &downloadResult{timeAtFirstPlace: now, name: segment.URI, seqNo: pl.SeqNo, status: "200 OK"}
					seenAtFirst.Add(segment.URI)
					continue
				}
				if seen.Contains(segment.URI) {
					continue
				}
				seen.Add(segment.URI)
				lastTimeNewSegmentSeen = time.Now()
				if time.Since(lastTimeDownloadStarted) < 50*time.Millisecond {
					time.Sleep(50 * time.Millisecond)
				}
				dTask := &downloadTask{baseURL: ms.u, url: segment.URI, seqNo: pl.SeqNo + uint64(i), title: segment.Title, duration: segment.Duration, appTime: now}
				go downloadSegment(dTask, ms.downloadResults)
				lastTimeDownloadStarted = time.Now()
				now = now.Add(time.Millisecond)
				// glog.V(model.VERBOSE).Infof("segment %s is of length %f seqId=%d", segment.URI, segment.Duration, segment.SeqId)
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func downloadSegment(task *downloadTask, res chan *downloadResult) {
	purl, err := url.Parse(task.url)
	if err != nil {
		glog.Fatal(err)
	}
	fsurl := task.url
	if !purl.IsAbs() {
		fsurl = task.baseURL.ResolveReference(purl).String()
	}
	try := 0
	for {
		// glog.V(model.DEBUG).Infof("Downloading segment seqNo=%d url=%s try=%d", task.seqNo, fsurl, try)
		glog.Infof("Downloading segment seqNo=%d url=%s try=%d", task.seqNo, fsurl, try)
		resp, err := httpClient.Get(fsurl)
		if err != nil {
			glog.Errorf("Error downloading %s: %v", fsurl, err)
			if try < 4 {
				try++
				continue
			}
			res <- &downloadResult{status: err.Error(), try: try}
			return
		}
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			glog.Errorf("Error downloading reading body %s: %v", fsurl, err)
			if try < 4 {
				try++
				resp.Body.Close()
				continue
			}
			res <- &downloadResult{status: err.Error(), try: try}
			return
		}
		resp.Body.Close()
		completedAt := time.Now()
		if resp.StatusCode != http.StatusOK {
			glog.Errorf("Error status downloading segment %s result status %s", fsurl, resp.Status)
			if try < 8 {
				try++
				time.Sleep(time.Second)
				continue
			}
			res <- &downloadResult{status: resp.Status, try: try}
			return
		}
		// glog.Infof("Download %s result: %s len %d", fsurl, resp.Status, len(b))
		fsttim, dur, verr := utils.GetVideoStartTimeAndDur(b)
		if verr != nil {
			msg := fmt.Sprintf("Error parsing video data %s result status %s video data len %d err %v",
				fsurl, resp.Status, len(b), verr)
			messenger.SendFatalMessage(msg)
			_, sn := path.Split(fsurl)
			glog.Infof("==============>>>>>>>>>>>>>  Saving segment %s", sn)
			ioutil.WriteFile(sn, b, 0644)
			sid := strconv.FormatInt(time.Now().Unix(), 10)
			if savedName, serr := save2GS(sid+"_"+task.url, b); serr != nil {
				messenger.SendFatalMessage(fmt.Sprintf("Failure to save segment to Google Storage %v", serr))
			} else {
				messenger.SendMessage(fmt.Sprintf("Segment %s (which can't be parsed) saved to Google Storage %s", task.url, savedName))
			}
		}
		// _, sn := path.Split(fsurl)
		// glog.Infof("==============>>>>>>>>>>>>>  Saving segment %s", sn)
		// ioutil.WriteFile(sn, b, 0644)
		// glog.V(model.DEBUG).Infof("Download %s result: %s len %d timeStart %s segment duration %s", fsurl, resp.Status, len(b), fsttim, dur)
		glog.Infof("Download %s result: %s len %d timeStart %s segment duration %s", fsurl, resp.Status, len(b), fsttim, dur)
		res <- &downloadResult{status: resp.Status, bytes: len(b), try: try, name: task.url, seqNo: task.seqNo,
			videoParseError: verr, startTime: fsttim, duration: dur, mySeqNo: task.mySeqNo, appTime: task.appTime, downloadCompetedAt: completedAt}
		// glog.Infof("Download %s result: %s len %d timeStart %s segment duration %s sent to channel", fsurl, resp.Status, len(b), fsttim, dur)
		return
	}
}
