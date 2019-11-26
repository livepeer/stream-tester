package testers

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"sort"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/m3u8"
	"github.com/livepeer/stream-tester/internal/messenger"
	"github.com/livepeer/stream-tester/internal/model"
	"github.com/livepeer/stream-tester/internal/utils"
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
	}

	nameAndURI struct {
		name string
		uri  *url.URL
	}
)

func newM3utester2(u string, wowzaMode bool, done chan struct{}, waitForTarget time.Duration) *m3utester2 {
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
	}
	go mut.workerLoop()
	go mut.manifestPullerLoop(waitForTarget)
	return mut
}

func newM3uMediaStream(name, resolution string, u *url.URL, wowzaMode bool, done chan struct{}, masterDR chan *downloadResult) *m3uMediaStream {
	ms := &m3uMediaStream{
		finite: finite{
			done: done,
		},
		name:             name,
		u:                u,
		resolution:       resolution,
		downloadResults:  make(chan *downloadResult, 32),
		streamSwitchedTo: make(chan nameAndURI),
	}
	go ms.workerLoop(masterDR)
	go ms.manifestPullerLoop(wowzaMode)
	return ms
}

func (mut *m3utester2) workerLoop() {
	results := make(map[string]downloadResultsByAppTime)
	for {
		select {
		case <-mut.done:
			return
		case dres := <-mut.downloadResults:
			// glog.Infof("downloaded: %+v", dres)
			continue
			if _, has := results[dres.resolution]; !has {
				results[dres.resolution] = make(downloadResultsByAppTime, 0, 128)
			}
			results[dres.resolution] = append(results[dres.resolution], dres)
			sort.Sort(results[dres.resolution])
			if len(results[dres.resolution]) > 128 {
				results[dres.resolution] = results[dres.resolution][1:]
			}
			// check for time drift
			if dres.seqNo < 24 {
				continue
			}
			for resolution, ress := range results {
				if resolution == dres.resolution {
					continue
				}
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
}

// continiously pull main manifest, starts new media streams
// or notifies existing media stream about stream change
func (mut *m3utester2) manifestPullerLoop(waitForTarget time.Duration) {
	surl := mut.initialURL.String()
	startedAt := time.Now()
	var gotManifest bool
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
		glog.Infof("Main manifest status %v", resp.Status)
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
		if lastNumberOfStreamsInManifest != len(mpl.Variants) {
			glog.Infof("Got playlist with %d variants (%s):", len(mpl.Variants), surl)
		}
		lastNumberOfStreamsInManifest = len(mpl.Variants)
		glog.V(model.VERBOSE).Infof("Got playlist with %d variants (%s):", len(mpl.Variants), surl)
		glog.V(model.VERBOSE).Info(mpl)
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
			stream := newM3uMediaStream(variant.URI, variant.Resolution, pvrui, mut.wowzaMode, mut.done, mut.downloadResults)
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

func (ms *m3uMediaStream) workerLoop(masterDR chan *downloadResult) {
	results := make(downloadResultsByAppTime, 0, 128)
	for {
		select {
		case <-ms.done:
			return
		case dres := <-ms.downloadResults:
			/*
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
			*/
			dres.resolution = ms.resolution
			glog.Infof("downloaded: %+v", dres)
			if dres.videoParseError != nil {
				msg := fmt.Sprintf("Error parsing video segment: %v", dres.videoParseError)
				ms.fatalEnd(msg)
				return
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
				if i < len(results)-1 {
					ns := results[i+1]
					tillNext = ns.startTime - r.startTime
					if tillNext > 0 && !isTimeEqualM(r.duration, tillNext) {
						problem = fmt.Sprintf(" ===> possible gap - to big time difference %s", r.duration-tillNext)
						print = true
					}
				}
				msg := fmt.Sprintf("%10s %14s seq %3d: mySeq %3d time %s duration %s till next %s appearance time %s %s\n",
					ms.resolution, r.name, r.seqNo, r.mySeqNo, r.startTime, r.duration, tillNext, r.appTime, problem)
				res += msg
				// if problem != "" && i < len(results)-6 {
				// 	fmt.Println(res)
				// 	ms.fatalEnd(msg)
				// 	return
				// }
				if problem != "" && i < len(results)-6 && fatalProblem == "" {
					fatalProblem = msg
					return
				}
			}
			if print {
				fmt.Println(res)
			}
			if fatalProblem != "" {
				ms.fatalEnd(fatalProblem)
				return
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

		glog.Infof("Got media playlist %s with %d (really %d) segments of url %s:", ms.resolution, len(pl.Segments), countSegments(pl), surl)
		glog.Info(pl)
		now := time.Now()
		for i, segment := range pl.Segments {
			if segment != nil {
				// glog.Infof("Segment: %+v", *segment)
				if wowzaMode {
					// remove Wowza's session id from URL
					segment.URI = wowzaSessionRE.ReplaceAllString(segment.URI, "_")
				}
				if seen.Contains(segment.URI) {
					// if i == 0 {
					// 	ms.downloadResults <- &downloadResult{timeAtFirstPlace: now, name: segment.URI, seqNo: pl.SeqNo}
					// }
					continue
				}
				seen.Add(segment.URI)
				lastTimeNewSegmentSeen = time.Now()
				dTask := &downloadTask{baseURL: ms.u, url: segment.URI, seqNo: pl.SeqNo + uint64(i), title: segment.Title, duration: segment.Duration, appTime: now}
				go downloadSegment(dTask, ms.downloadResults)
				now = now.Add(time.Millisecond)
				// glog.V(model.VERBOSE).Infof("segment %s is of length %f seqId=%d", segment.URI, segment.Duration, segment.SeqId)
			}
		}
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
		glog.V(model.DEBUG).Infof("Downloading segment seqNo=%d url=%s try=%d", task.seqNo, fsurl, try)
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
		fsttim, dur, verr := utils.GetVideoStartTimeAndDur(b)
		if verr != nil {
			glog.Errorf("Error parsing video data %s result status %s video data len %d err %v", fsurl, resp.Status, len(b), err)
			_, sn := path.Split(fsurl)
			glog.Infof("==============>>>>>>>>>>>>>  Saving segment %s", sn)
			ioutil.WriteFile(sn, b, 0644)
		}
		glog.V(model.DEBUG).Infof("Download %s result: %s len %d timeStart %s segment duration %s", fsurl, resp.Status, len(b), fsttim, dur)
		res <- &downloadResult{status: resp.Status, bytes: len(b), try: try, name: task.url, seqNo: task.seqNo,
			videoParseError: verr, startTime: fsttim, duration: dur, mySeqNo: task.mySeqNo, appTime: task.appTime}
		return
	}
}
