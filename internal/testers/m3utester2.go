package testers

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/joy4/jerrors"
	"github.com/livepeer/m3u8"
	"github.com/livepeer/stream-tester/internal/metrics"
	"github.com/livepeer/stream-tester/internal/utils"
	"github.com/livepeer/stream-tester/internal/utils/uhttp"
	"github.com/livepeer/stream-tester/messenger"
	"github.com/livepeer/stream-tester/model"
)

// IgnoreGaps ...
var IgnoreGaps bool

// IgnoreTimeDrift ...
var IgnoreTimeDrift bool

const (
	// reportStatsEvery = 5 * time.Minute
	reportStatsEvery = 30 * time.Second
	// reportStatsEvery  = 4 * 60 * time.Second
	reportStatsEvery2 = 30 * time.Minute
	// timeFormat        = "2006-01-02T15:04:05.9999"
	timeFormat = "15:04:05.9999"
)

type (
	resolution string

	finite struct {
		ctx         context.Context
		cancel      context.CancelFunc
		globalError error
	}
	// m3utester2 tests one stream, reading all the media streams
	// was designed for use with non-go-livepeer node RTMP ingesters
	// like Wowza and Mist
	m3utester2 struct {
		finite
		initialURL             *url.URL
		wowzaMode              bool
		mistMode               bool
		save                   bool
		followRename           bool
		printStats             bool
		failIfTranscodingStops bool
		streams                map[resolution]*m3uMediaStream
		driftCheckResults      chan *downloadResult
		latencyResults         chan *latencyResult
		segmentsMatcher        *segmentsMatcher
		savePlayList           *m3u8.MasterPlaylist
		savePlayListName       string
		saveDirName            string
		sourceRes              string
		stats                  model.Stats1
		mu                     sync.Mutex
		globalError            error
		allResults             map[string][]*downloadResult
	}

	// m3uMediaStream downloads media stream. Hadle stream changes
	// (must be notified about new url by m3utester2)
	m3uMediaStream struct {
		finite
		name                   string // usually medial playlist relative name
		resolution             string
		u                      *url.URL
		downloadResults        chan *downloadResult
		streamSwitchedTo       chan nameAndURI
		segmentsMatcher        *segmentsMatcher
		downTasks              chan downloadTask
		segmentsToDownload     int32
		segmentsDownloaded     int32
		failIfTranscodingStops bool
		isFinite               bool
		save                   bool
		savePlayList           *m3u8.MediaPlaylist
		savePlayListName       string
		saveDirName            string
	}

	nameAndURI struct {
		name string
		uri  *url.URL
	}

	latencyResult struct {
		resolution  string
		seqNo       uint64
		name        string
		latency     time.Duration
		speedRatio  float64
		successRate float64
	}
)

// NewM3utester2 pubic method
func NewM3utester2(pctx context.Context, u string, wowzaMode, mistMode, failIfTranscodingStops, save bool,
	waitForTarget time.Duration, sm *segmentsMatcher) model.IVODTester {

	return newM3utester2(pctx, u, wowzaMode, mistMode, failIfTranscodingStops, save, true, waitForTarget, sm)
}

func newM3utester2(pctx context.Context, u string, wowzaMode, mistMode, failIfTranscodingStops, save, printStats bool,
	waitForTarget time.Duration, sm *segmentsMatcher) *m3utester2 {

	iu, err := url.Parse(u)
	if err != nil {
		glog.Fatal(err)
	}
	ctx, cancel := context.WithCancel(pctx)
	mut := &m3utester2{
		finite: finite{
			ctx:    ctx,
			cancel: cancel,
		},
		initialURL:             iu,
		wowzaMode:              wowzaMode,
		mistMode:               mistMode,
		save:                   save,
		printStats:             printStats,
		followRename:           wowzaMode,
		failIfTranscodingStops: failIfTranscodingStops,
		streams:                make(map[resolution]*m3uMediaStream),
		driftCheckResults:      make(chan *downloadResult, 32),
		latencyResults:         make(chan *latencyResult, 32),
		segmentsMatcher:        sm,
		allResults:             make(map[string][]*downloadResult),
	}
	mut.stats.Started = true
	go mut.workerLoop()
	go mut.manifestPullerLoop(waitForTarget)
	return mut
}

func (mut *m3utester2) VODStats() model.VODStats {
	vs := model.VODStats{
		SegmentsNum: make(map[string]int),
		SegmentsDur: make(map[string]time.Duration),
	}
	glog.Infof("==> all results: %+v", mut.allResults)
	for resolution, drs := range mut.allResults {
		if mut.sourceRes == resolution {
		} else {
		}
		for _, seg := range drs {
			vs.SegmentsDur[resolution] += seg.duration
			vs.SegmentsNum[resolution]++
			vs.SegmentsAll++
			vs.DurationAll += seg.duration
			if seg.videoParseError != nil {
				vs.ParseErrors++
			}
		}

	}
	return vs
}

func (mut *m3utester2) doSavePlaylist() {
	if mut.savePlayList != nil {
		err := ioutil.WriteFile(mut.savePlayListName, mut.savePlayList.Encode().Bytes(), 0644)
		if err != nil {
			glog.Fatal(err)
		}
	}
}

func (mut *m3utester2) initSave(streamURL, mediaURL string) {
	mut.savePlayList = m3u8.NewMasterPlaylist()
	var streamName string
	var err error
	if streamURL != "" {
		if streamName, err = parseStreamURL(streamURL); err != nil {
			glog.Fatal(err)
		}
	} else {
		if streamName, _, err = parseMediaURL(mediaURL); err != nil {
			glog.Fatal(err)
		}
	}
	mut.saveDirName = streamName
	mut.savePlayListName = filepath.Join(mut.saveDirName, streamName+".m3u8")
	if _, err := os.Stat(mut.saveDirName); os.IsNotExist(err) {
		os.Mkdir(mut.saveDirName, 0755)
	}
	glog.Infof("Save dir name: '%s', main playlist save name %s", mut.saveDirName, mut.savePlayListName)
}

func newM3uMediaStream(ctx context.Context, cancel context.CancelFunc, name, resolution string, u *url.URL, wowzaMode bool, masterDR chan *downloadResult,
	sm *segmentsMatcher, latencyResults chan *latencyResult, save, failIfTranscodingStops bool) *m3uMediaStream {

	ms := &m3uMediaStream{
		finite: finite{
			ctx:    ctx,
			cancel: cancel,
		},
		name:                   name,
		u:                      u,
		save:                   save,
		failIfTranscodingStops: failIfTranscodingStops,
		resolution:             resolution,
		downloadResults:        make(chan *downloadResult, 32),
		streamSwitchedTo:       make(chan nameAndURI),
		segmentsMatcher:        sm,
		downTasks:              make(chan downloadTask, 256),
	}
	go ms.workerLoop(masterDR, latencyResults)
	go ms.manifestPullerLoop(wowzaMode)
	for i := 0; i < simultaneousDownloads; i++ {
		go ms.segmentDownloadWorker(i)
	}
	if ms.save {
		streamName, mediaStreamName, err := parseMediaURL(u.String())
		if err != nil {
			glog.Fatal(err)
		}
		ms.saveDirName = filepath.Join(streamName, mediaStreamName)
		ms.savePlayListName = filepath.Join(ms.saveDirName, mediaStreamName+".m3u8")
		if _, err := os.Stat(ms.saveDirName); os.IsNotExist(err) {
			os.Mkdir(ms.saveDirName, 0755)
		}
		glog.Infof("Save dir name: '%s', main playlist save name %s", ms.saveDirName, ms.savePlayListName)
		mpl, err := m3u8.NewMediaPlaylist(0, 1024)
		mpl.MediaType = m3u8.VOD
		mpl.Live = false
		if err != nil {
			panic(err)
		}
		ms.savePlayList = mpl
	}
	return ms
}

func (mut *m3utester2) Stats() model.Stats1 {
	mut.mu.Lock()
	stats := mut.stats
	mut.mu.Unlock()
	return stats
}

func (mut *m3utester2) workerLoop() {
	seenResolutions := make(sort.StringSlice, 0, 16)
	results := make(map[string][]*downloadResult)
	started := time.Now()
	timer := time.NewTimer(reportStatsEvery)
	defer timer.Stop()
	finishCheckTimer := time.NewTicker(5 * time.Second)
	defer finishCheckTimer.Stop()
	// transcodedLatencies := utils.LatenciesCalculator{}
	latencies := make(map[string]*utils.DurationsCapped)
	sourceLatencies := utils.NewDurations(4096)
	transcodeLatencies := utils.NewDurations(4 * 4096)
	succRates := make(map[string]float64)
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
		case <-mut.ctx.Done():
			if mut.printStats {
				printStats()
			}
			return
		case <-finishCheckTimer.C:
			allFinished := len(mut.streams) > 0
			for _, stream := range mut.streams {
				if !stream.isFiniteDownloadsFinished() {
					allFinished = false
					break
				}
			}
			if allFinished {
				// all VOD media streams downloads finished, stop downloading
				glog.Infof("all VOD media streams downloads finished, stop downloading")
				mut.Cancel()
				return
			}
		case <-timer.C:
			if mut.printStats {
				printStats()
			}
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
			if lres.successRate > 0 {
				mut.mu.Lock()
				succRates[lres.resolution] = lres.successRate
				var sum float64
				for _, sr := range succRates {
					sum += sr
				}
				mut.stats.SuccessRate = sum / float64(len(succRates))
				if mut.sourceRes == lres.resolution {
					sourceLatencies.Add(lres.latency)
				} else {
					transcodeLatencies.Add(lres.latency)
				}
				avg, p50, p95, p99 := sourceLatencies.Calc()
				mut.stats.SourceLatencies = model.Latencies{Avg: avg, P50: p50, P95: p95, P99: p99}
				avg, p50, p95, p99 = transcodeLatencies.Calc()
				mut.stats.TranscodedLatencies = model.Latencies{Avg: avg, P50: p50, P95: p95, P99: p99}
				mut.mu.Unlock()
			}

		case dres := <-mut.driftCheckResults:
			if dres.timeAtFirstPlace.IsZero() {
				glog.V(model.VVERBOSE).Infof("MAIN downloaded: %s", dres.String2())
				// if _, has := mut.allResults[dres.resolution]; !has {
				// 	results[dres.resolution] = make([]*downloadResult, 0, 128)
				// }
				mut.allResults[dres.resolution] = append(mut.allResults[dres.resolution], dres)
				continue
			}
			if _, has := results[dres.resolution]; !has {
				results[dres.resolution] = make([]*downloadResult, 0, 128)
				seenResolutions = append(seenResolutions, dres.resolution)
				seenResolutions.Sort()
			}
			results[dres.resolution] = append(results[dres.resolution], dres)
			sort.Slice(results[dres.resolution], func(i, j int) bool {
				return results[dres.resolution][i].timeAtFirstPlace.Before(results[dres.resolution][j].timeAtFirstPlace)
			})
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
								mut.fatalEnd(errors.New(msg))
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

func (f *finite) fatalEnd(err error) {
	f.globalError = err
	model.ExitCode = 127
	// glog.Error(msg)
	messenger.SendFatalMessage(err.Error())
	f.cancel()
	// panic(msg)
}

func (f *finite) Done() <-chan struct{} {
	return f.ctx.Done()
}

func (f *finite) Cancel() {
	f.cancel()
}

func (f *finite) Finished() bool {
	select {
	case <-f.ctx.Done():
		return true
	default:
		return false
	}
}

func getURLPath(surl string) ([]string, error) {
	pu, err := url.Parse(surl)
	if err != nil {
		return nil, err
	}
	pp := strings.Split(strings.Trim(pu.Path, "/"), "/")
	if len(pp) == 0 {
		return nil, fmt.Errorf("Empty path %s", surl)
	}
	if pp[len(pp)-1] == "index.m3u8" {
		pp = pp[:len(pp)-1]
	}
	if len(pp) == 0 {
		return nil, fmt.Errorf("Empty path %s", surl)
	}
	return pp, nil
}

func parseStreamURL(surl string) (string, error) {
	pp, err := getURLPath(surl)
	if err != nil {
		return "", err
	}
	streamName := strings.TrimSuffix(pp[len(pp)-1], ".m3u8")
	return streamName, nil
}

func parseMediaURL(surl string) (string, string, error) {
	pp, err := getURLPath(surl)
	if err != nil {
		return "", "", err
	}
	if len(pp) < 2 {
		return "", "", fmt.Errorf("Can't parse media stream name from %s", surl)
	}
	streamName := strings.TrimSuffix(pp[len(pp)-2], ".m3u8")
	mediaName := strings.TrimSuffix(pp[len(pp)-1], ".m3u8")
	return streamName, mediaName, nil
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
		case <-mut.ctx.Done():
			return
		default:
		}
		if waitForTarget > 0 && !gotManifest && time.Since(startedAt) > waitForTarget {
			mut.fatalEnd(fmt.Errorf("Can't get playlist %s for %s, giving up.", surl, time.Since(startedAt)))
			return
		}
		// glog.Infof("requesting %s", surl)
		resp, err := httpClient.Do(uhttp.GetRequest(surl))
		// glog.Infof("DONE requesting %s", surl)
		if err != nil {
			uerr := err.(*url.Error)
			if uerr.Timeout() {
				countTimeouts++
				if countTimeouts > 32 {
					mut.fatalEnd(fmt.Errorf("Fatal timeout error trying to get playlist %s: %v", surl, err))
					return
				}
				time.Sleep(2 * time.Second)
				continue
			}
			mut.fatalEnd(fmt.Errorf("Fatal error trying to get playlist %s: %v", surl, err))
			return
		}
		countTimeouts = 0
		if resp.StatusCode != http.StatusOK {
			b, _ := ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			glog.V(model.VVERBOSE).Infof("===== status error getting master playlist %s: %v (%s) body: %s", surl, resp.StatusCode, resp.Status, string(b))
			time.Sleep(2 * time.Second)
			continue
		}
		// glog.Infof("Main manifest status %v", resp.Status)
		b, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		glog.V(model.INSANE).Infof("Main manifest status %v", resp.Status)
		glog.V(model.INSANE2).Info(string(b))
		if err != nil {
			glog.Error("===== error getting master playlist: ", err)
			// glog.Error(err)
			time.Sleep(2 * time.Second)
			continue
		}
		if strings.Contains(string(b), "#EXT-X-ERROR") {
			glog.Error("===== error in playlist: ", string(b))
			time.Sleep(2 * time.Second)
			continue
		}
		gpl, plt, err := m3u8.Decode(*bytes.NewBuffer(b), true)
		// glog.Infof("playlist type is %d", plt)
		// err = mpl.DecodeFrom(resp.Body, true)
		// mpl := m3u8.NewMasterPlaylist()
		// err = mpl.Decode(*bytes.NewBuffer(b), true)
		if err != nil {
			glog.Error("===== error parsing master playlist: ", err)
			// glog.Error(err)
			time.Sleep(2 * time.Second)
			continue
		}
		if plt == m3u8.MEDIA {
			// we got URL to the media playlist
			mres := "media"
			_, mediaName, err := parseMediaURL(surl)
			if err != nil {
				mut.fatalEnd(fmt.Errorf("Error: %s", err))
				return
			}
			// _, name := filepath.Split(surl)
			if mut.save {
				mut.initSave("", surl)
				mut.savePlayList.Append(mediaName+"/"+mediaName+".m3u8", nil, m3u8.VariantParams{Name: mediaName})
				mut.doSavePlaylist()
			}
			stream := newM3uMediaStream(mut.ctx, mut.cancel, mediaName, mres, mut.initialURL, mut.wowzaMode, mut.driftCheckResults, mut.segmentsMatcher, mut.latencyResults,
				mut.save, mut.failIfTranscodingStops)
			mut.streams[resolution(mres)] = stream
			return
		}
		if !gotManifest && mut.save {
			mut.initSave(surl, "")
		}
		gotManifest = true
		mpl := gpl.(*m3u8.MasterPlaylist)
		glog.V(model.VVERBOSE).Infof("Got playlist with %d variants (%s):", len(mpl.Variants), surl)
		glog.V(model.VVERBOSE).Info(mpl)
		if lastNumberOfStreamsInManifest != len(mpl.Variants) {
			glog.V(model.DEBUG).Infof("Got playlist with %d variants (%s):", len(mpl.Variants), surl)
		}
		// glog.Infof("==> model.ProfilesNum = %d", model.ProfilesNum)
		if !streamStarted && len(mpl.Variants) >= model.ProfilesNum+1 {
			streamStarted = true
		} else if waitForTarget > 0 && !streamStarted && time.Since(startedAt) > 2*waitForTarget {
			msg := fmt.Sprintf("Stream started %s ago, but main manifest still has %d profiles instead of %d. Stopping.",
				time.Since(startedAt), len(mpl.Variants), model.ProfilesNum+1)
			glog.Error(msg)
			mut.fatalEnd(errors.New(msg))
			return
		}
		lastNumberOfStreamsInManifest = len(mpl.Variants)
		// glog.Infof("Got playlist with %d variants (%s):", len(mpl.Variants), surl)
		// glog.Info(mpl)
		seenResolution := newStringRing(len(mpl.Variants))
		needSavePlaylist := false
		for _, variant := range mpl.Variants {
			// glog.Infof("Variant URI: %s", variant.URI)
			if mut.wowzaMode {
				// remove Wowza's session id from URL
				variant.URI = wowzaSessionRE.ReplaceAllString(variant.URI, "_")
			}
			if mut.mistMode {
				variant.URI = mistSessionRE.ReplaceAllString(variant.URI, "")
			}
			ress := variant.Resolution
			if !mut.followRename {
				ress = variant.URI
			} else {
				if variant.Resolution == "" {
					msg := fmt.Sprintf("Got variant %s without resolution, stopping", variant.URI)
					mut.fatalEnd(errors.New(msg))
					return
				}
				if seenResolution.Contains(variant.Resolution) {
					msg := fmt.Sprintf("Playlist has resolution %s more than once, stopping", variant.Resolution)
					mut.fatalEnd(errors.New(msg))
					return
				}
			}
			seenResolution.Add(ress)
			res := resolution(ress)

			pvrui, err := url.Parse(variant.URI)
			if err != nil {
				msg := fmt.Sprintf("Error parsing variant url %s: %v", variant.URI, err)
				mut.fatalEnd(errors.New(msg))
				return
			}
			// glog.Infof("Parsed uri: %+v", pvrui, pvrui.IsAbs)
			if !pvrui.IsAbs() {
				pvrui = mut.initialURL.ResolveReference(pvrui)
			}
			if ms, has := mut.streams[res]; has {
				if ms.name != variant.URI && mut.followRename {
					// stream changed, notify stream downloaded
					ms.streamSwitchedTo <- nameAndURI{name: variant.URI, uri: pvrui} // blocks until processed, needed to make sure name was changed
				}
				continue
			}

			glog.V(model.VVERBOSE).Infof("Media url=%s", pvrui.String())
			if mut.sourceRes == "" {
				mut.sourceRes = ress
			}
			stream := newM3uMediaStream(mut.ctx, mut.cancel, variant.URI, ress, pvrui, mut.wowzaMode, mut.driftCheckResults,
				mut.segmentsMatcher, mut.latencyResults, mut.save, mut.failIfTranscodingStops)
			mut.streams[res] = stream
			if mut.save {
				needSavePlaylist = true
				_, mediaName, err := parseMediaURL(pvrui.String())
				if err != nil {
					glog.Fatal(err)
				}
				mut.savePlayList.Append(mediaName+"/"+mediaName+".m3u8", nil, variant.VariantParams)
			}
		}
		if needSavePlaylist {
			mut.doSavePlaylist()
		}
		time.Sleep(2 * time.Second)
	}
}

func (ms *m3uMediaStream) segmentDownloadWorker(num int) {
	resultsChan := make(chan *downloadResult)
	for {
		select {
		case <-ms.ctx.Done():
			return
		case task := <-ms.downTasks:
			segmentsDownloading := metrics.Census.IncSegmentsDownloading()
			glog.V(model.INSANE).Infof("Worker %d got task to download: seqNo=%d downloading=%d url=%s", num, task.seqNo, segmentsDownloading, task.url)
			started := time.Now()
			go downloadSegment(&task, resultsChan)
			res := <-resultsChan
			segmentsDownloading = metrics.Census.SegmentDownloaded()
			glog.V(model.INSANE).Infof("Worker %d FINISHED download task: seqNo=%d downloading=%d url=%s took=%s", num, task.seqNo, segmentsDownloading, task.url, time.Since(started))
			ms.downloadResults <- res
			atomic.AddInt32(&ms.segmentsDownloaded, 1)
		}
	}
}

func roundSucc(v float64) float64 {
	return math.Round(v*1000.0) / 1000.0
}

func (ms *m3uMediaStream) workerLoop(masterDR chan *downloadResult, latencyResults chan *latencyResult) {
	results := make([]*downloadResult, 0, 128)
	var lastPrintTime, lastMessageSentAt time.Time

	var successRate float64
	var downloadedSegmentsTotalDuration time.Duration
	var firstSegmentPTS time.Duration = -1

	for {
		select {
		case <-ms.ctx.Done():
			return
		case dres := <-ms.downloadResults:
			// desc := fmt.Sprintf("== ms loop downloaded status %s res %s name %s seqNo %d len %d", dres.status, dres.resolution, dres.name, dres.seqNo, dres.bytes)
			if dres.status != "200 OK" {
				continue
			}
			if !dres.timeAtFirstPlace.IsZero() {
				for i := len(results) - 1; i >= 0; i-- {
					r := results[i]
					if r.seqNo == dres.seqNo && dres.name == r.name {
						r.timeAtFirstPlace = dres.timeAtFirstPlace
						masterDR <- r
						break
					}
				}
				continue
			}
			dres.resolution = ms.resolution
			if dres.duration > 0 {
				downloadedSegmentsTotalDuration += dres.duration
				if firstSegmentPTS == -1 {
					firstSegmentPTS = dres.startTime
				} else if dres.startTime < firstSegmentPTS {
					firstSegmentPTS = dres.startTime
				}
			}
			if ms.segmentsMatcher != nil && dres.duration > 0 {
				rft, rl2t, rl1t, rlt := ms.segmentsMatcher.getStartEnd()
				glog.V(model.INSANE).Infof("stream %s rft %v rl2t %s rl1t %s rlt %s", ms.resolution, rft, rl2t, rl1t, rlt)
				segLast := dres.startTime + dres.duration
				rtmpLast := rl2t
				if rtmpLast <= segLast {
					rtmpLast = rl1t
				}
				if rtmpLast <= segLast {
					rtmpLast = rl2t
				}
				rdur := rtmpLast - rft - firstSegmentPTS
				successRate = float64(downloadedSegmentsTotalDuration) / float64(rdur)
				successRateRounded := roundSucc(successRate)
				glog.V(model.VVERBOSE).Infof("stream %s rtmp dur %s hls dur %s succ rate %v succ rate rounded %v first seg %s last rtmp %s", ms.resolution,
					rdur, downloadedSegmentsTotalDuration, successRate, successRateRounded, firstSegmentPTS, rlt)
				successRate = successRateRounded
			}
			// dres.data = nil
			// glog.Infof("downloaded: %+v", *dres)
			// glog.Infof("downloaded task: %+v", *dres.task)
			if ms.save {
				_, segFileName := filepath.Split(dres.name)
				fullSegFileName := filepath.Join(ms.saveDirName, segFileName)
				seg := new(m3u8.MediaSegment)
				seg.URI = segFileName
				seg.SeqId = dres.task.seqNo
				seg.Duration = dres.task.duration
				seg.Title = dres.task.title
				if err := ms.insertSegmentToSavePlaylist(dres.task.seqNo, seg); err != nil {
					glog.Fatal(err)
				}
				if err := ioutil.WriteFile(ms.savePlayListName, ms.savePlayList.Encode().Bytes(), 0644); err != nil {
					glog.Fatal(err)
				}
				go func(segFileName, fullpath string, b []byte) {
					if err := ioutil.WriteFile(fullpath, b, 0644); err != nil {
						glog.Fatal(err)
					}
					glog.V(model.VVERBOSE).Infof("Segment %s saved to %s", segFileName, fullpath)
				}(segFileName, fullSegFileName, dres.data)
			}
			dres.data = nil
			dres.task = nil

			if dres.videoParseError != nil {
				msg := fmt.Sprintf("Error parsing video segment: %v", dres.videoParseError)
				if IgnoreNoCodecError && (errors.Is(dres.videoParseError, jerrors.ErrNoAudioInfoFound) || errors.Is(dres.videoParseError, jerrors.ErrNoVideoInfoFound)) {
					messenger.SendFatalMessage(msg)
					continue
				} else {
					ms.fatalEnd(errors.New(msg))
					return
				}
			}
			var latency time.Duration
			var speedRatio float64
			var merr error
			if ms.segmentsMatcher != nil {
				latency, speedRatio, merr = ms.segmentsMatcher.matchSegment(dres.startTime, dres.duration, dres.downloadCompetedAt)
			}
			glog.V(model.DEBUG).Infof(`%s seqNo %4d name=%s latency is %s speedRatio is %v`, dres.resolution, dres.seqNo, dres.name, latency, speedRatio)
			if merr != nil {
				glog.Infof("downloaded: %+v, segment matching error %v", dres, merr)
				// TODO investigate why getting this with Mist server
				// messenger.SendFatalMessage(merr.Error())
				// panic(merr)
				continue
			}
			if ms.segmentsMatcher != nil {
				latencyResults <- &latencyResult{name: dres.name, resolution: ms.resolution, seqNo: dres.seqNo, latency: latency,
					speedRatio: speedRatio, successRate: successRate}
			}
			masterDR <- dres
			results = append(results, dres)
			sort.Slice(results, func(i, j int) bool {
				return results[i].appTime.Before(results[j].appTime)
			})
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
				if i < len(results)-1 && time.Since(r.downloadStartedAt) > HTTPTimeout {
					ns := results[i+1]
					tillNext = ns.startTime - r.startTime
					if tillNext > 0 && !isTimeEqualM(r.duration, tillNext) {
						// problem = fmt.Sprintf(" ===> possible gap - to big time difference %s (d2 i: %d, now %d) (because of %s)", tillNext-r.duration, i, time.Now().UnixNano(), desc)
						problem = fmt.Sprintf(" ===> possible gap - to big time difference %s", tillNext-r.duration)
						print = true
					}
				}
				msg := fmt.Sprintf("%10s %14s seq %3d: time %s duration %7s till next %7s appeared %s down start %s %s\n",
					ms.resolution, r.name, r.seqNo, r.startTime, r.duration, tillNext, r.appTime.Local().Format(timeFormat),
					r.downloadStartedAt.Local().Format(timeFormat), problem)
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
					ms.fatalEnd(errors.New("\n" + fatalProblem))
					return
				}
				if time.Since(lastMessageSentAt) > 5*60*time.Second {
					messenger.SendMessage(fatalProblem)
					lastMessageSentAt = time.Now()

				}
				// messenger.SendMessageDebounced(fatalProblem)
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
	countResets := 0
	gotManifest := false
	for {
		select {
		case <-ms.ctx.Done():
			return
		case switched := <-ms.streamSwitchedTo:
			glog.Infof(`Stream %s (%s) switched to %s (%s)`, ms.name, surl, switched.name, switched.uri.String())
			ms.name = switched.name
			ms.u = switched.uri
			surl = switched.uri.String()
			lastTimeNewSegmentSeen = time.Now()
		default:
		}
		if ms.failIfTranscodingStops {
			if time.Since(lastTimeNewSegmentSeen) > 32*time.Second {
				msg := fmt.Sprintf("Stream %s not seen new segments for %s, stopping.", surl, time.Since(lastTimeNewSegmentSeen))
				ms.fatalEnd(errors.New(msg))
				return
			}
		}
		resp, err := httpClient.Do(uhttp.GetRequest(surl))
		if err != nil {
			uerr := err.(*url.Error)
			if uerr.Timeout() {
				countTimeouts++
				if countTimeouts > 15 {
					ms.fatalEnd(fmt.Errorf("Fatal timeout error trying to get media playlist %s: %v", surl, err))
					return
				}
				time.Sleep(2 * time.Second)
				continue
			}
			if strings.Contains(err.Error(), "connection reset by peer") {
				countResets++
				if countResets > 15 {
					ms.fatalEnd(fmt.Errorf("Fatal connection reset error trying to get media playlist %s: %v", surl, err))
					return
				}
				time.Sleep(2 * time.Second)
				continue
			}
			ms.fatalEnd(fmt.Errorf("Fatal error trying to get media playlist %s: %v", surl, err))
			return
		}
		countTimeouts = 0
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
			if strings.Contains(err.Error(), "connection reset by peer") {
				countResets++
				if countResets > 15 {
					ms.fatalEnd(fmt.Errorf("Fatal connection reset error trying to get media playlist %s: %v", surl, err))
					return
				}
				time.Sleep(2 * time.Second)
				continue
			}
			ms.fatalEnd(fmt.Errorf("Fatal error trying to read media playlist %s: %v", surl, err))
			return
		}
		countResets = 0

		gpl, plt, err := m3u8.Decode(*bytes.NewBuffer(b), true)
		if err != nil {
			glog.Fatal(err)
		}
		if plt != m3u8.MEDIA {
			glog.Fatalf("Expecting media playlist, got %d (url=%s)", plt, surl)
		}
		pl := gpl.(*m3u8.MediaPlaylist)
		// pl, err := m3u8.NewMediaPlaylist(100, 100)
		// if err != nil {
		// 	glog.Fatal(err)
		// }
		// err = pl.Decode(*bytes.NewBuffer(b), true)
		// if err != nil {
		// 	glog.Fatal(err)
		// }
		if !gotManifest && ms.save {
			ms.savePlayList.TargetDuration = pl.TargetDuration
			ms.savePlayList.SeqNo = pl.SeqNo
			gotManifest = true
		}
		glog.V(model.VVERBOSE).Infof("Got media playlist %s with %d (really %d (%d)) segments of url %s:", ms.resolution, len(pl.Segments), countSegments(pl), pl.Len(), surl)
		glog.V(model.INSANE2).Info(string(b))
		// glog.V(model.VVERBOSE).Info(pl)
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
					glog.V(model.INSANE).Infof("===> segment at first place %s (%s) seq %d", segment.URI, ms.resolution, pl.SeqNo)
					ms.downloadResults <- &downloadResult{timeAtFirstPlace: now, name: segment.URI, seqNo: pl.SeqNo, status: "200 OK"}
					seenAtFirst.Add(segment.URI)
					continue
				}
				if seen.Contains(segment.URI) {
					continue
				}
				seen.Add(segment.URI)
				lastTimeNewSegmentSeen = time.Now()
				if time.Since(lastTimeDownloadStarted) < 5*time.Millisecond {
					time.Sleep(5 * time.Millisecond)
				}
				segSeqNo := pl.SeqNo + uint64(i)
				glog.V(model.INSANE).Infof("===> adding task to download %s: %s seqNo=%d", ms.resolution, segment.URI, segSeqNo)
				ms.downTasks <- downloadTask{baseURL: ms.u, url: segment.URI, seqNo: segSeqNo, title: segment.Title, duration: segment.Duration, appTime: now}
				ms.segmentsToDownload++
				metrics.Census.IncSegmentsToDownload()
				lastTimeDownloadStarted = time.Now()
				now = now.Add(time.Millisecond)
				// glog.V(model.VERBOSE).Infof("segment %s is of length %f seqId=%d", segment.URI, segment.Duration, segment.SeqId)
			}
		}
		// pl.Live = false
		if pl.Len() > 0 && (!pl.Live || pl.MediaType == m3u8.EVENT) {
			// VoD and Event's should show the entire playlist
			glog.Infof("Playlist %s is VOD, so stopping manifest puller loop", surl)
			ms.isFinite = true
			return
		}

		delay := 2 * time.Second
		if ms.segmentsMatcher != nil {
			delay = 100 * time.Millisecond
		}
		time.Sleep(delay)
	}
}

func (ms *m3uMediaStream) isFiniteDownloadsFinished() bool {
	return ms.isFinite && ms.segmentsToDownload > 0 && ms.segmentsToDownload == atomic.LoadInt32(&ms.segmentsDownloaded)
}

func (ms *m3uMediaStream) insertSegmentToSavePlaylist(seqNo uint64, seg *m3u8.MediaSegment) error {
	var err error
	err = ms.savePlayList.InsertSegment(seqNo, seg)
	if err == m3u8.ErrPlaylistFull {
		mpl, err := m3u8.NewMediaPlaylist(0, uint(len(ms.savePlayList.Segments)*2))
		if err != nil {
			glog.Fatal(err)
		}
		mpl.TargetDuration = ms.savePlayList.TargetDuration
		mpl.SeqNo = ms.savePlayList.SeqNo
		mpl.MediaType = m3u8.VOD
		mpl.Live = false
		for _, oseg := range ms.savePlayList.Segments {
			if oseg != nil {
				if err = mpl.InsertSegment(oseg.SeqId, oseg); err != nil {
					glog.Fatal(err)
				}
			}
		}
		err = ms.savePlayList.InsertSegment(seqNo, seg)
	}
	return err
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
		glog.V(model.VERBOSE).Infof("Downloading segment seqNo=%d url=%s try=%d", task.seqNo, fsurl, try)
		// glog.Infof("Downloading segment seqNo=%d url=%s try=%d", task.seqNo, fsurl, try)
		start := time.Now()
		resp, err := httpClient.Do(uhttp.GetRequest(fsurl))
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
		resp.Body.Close()
		if err != nil {
			glog.V(model.VERBOSE).Infof("Error downloading reading body %s: %v", fsurl, err)
			if try < 4 {
				try++
				time.Sleep(50 * time.Millisecond)
				continue
			}
			res <- &downloadResult{status: err.Error(), try: try}
			return
		}
		completedAt := time.Now()
		if resp.StatusCode != http.StatusOK {
			glog.V(model.VERBOSE).Infof("Error status downloading segment %s result status %s", fsurl, resp.Status)
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
			if !(IgnoreNoCodecError && isNoCodecError(verr)) {
				messenger.SendFatalMessage(msg)
				_, sn := path.Split(fsurl)
				glog.V(model.DEBUG).Infof("==============>>>>>>>>>>>>>  Saving segment %s", sn)
				ioutil.WriteFile(sn, b, 0644)
				sid := strconv.FormatInt(time.Now().Unix(), 10)
				if savedName, service, serr := SaveToExternalStorage(sid+"_"+task.url, b); serr != nil {
					messenger.SendFatalMessage(fmt.Sprintf("Failure to save segment to %s %v", service, serr))
				} else {
					messenger.SendMessage(fmt.Sprintf("Segment %s (which can't be parsed) saved to %s %s", task.url, service, savedName))
				}
			}
		}
		// _, sn := path.Split(fsurl)
		// glog.Infof("==============>>>>>>>>>>>>>  Saving segment %s", sn)
		// ioutil.WriteFile(sn, b, 0644)
		// glog.V(model.DEBUG).Infof("Download %s result: %s len %d timeStart %s segment duration %s", fsurl, resp.Status, len(b), fsttim, dur)
		glog.V(model.DEBUG).Infof("Download %s result: %s len %d timeStart %s segment duration %s took=%s", fsurl, resp.Status, len(b), fsttim, dur, time.Since(start))
		res <- &downloadResult{status: resp.Status, bytes: len(b), try: try, name: task.url, seqNo: task.seqNo,
			videoParseError: verr, startTime: fsttim, duration: dur, mySeqNo: task.mySeqNo, appTime: task.appTime, downloadCompetedAt: completedAt,
			downloadStartedAt: start, data: b, task: task,
		}
		// glog.Infof("Download %s result: %s len %d timeStart %s segment duration %s sent to channel", fsurl, resp.Status, len(b), fsttim, dur)
		return
	}
}
