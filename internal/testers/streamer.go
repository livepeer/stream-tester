package testers

import (
	"context"
	"fmt"
	"math/rand"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/gosuri/uiprogress"

	"github.com/livepeer/stream-tester/apis/livepeer"
	"github.com/livepeer/stream-tester/apis/mist"
	"github.com/livepeer/stream-tester/internal/utils"
	"github.com/livepeer/stream-tester/messenger"
	"github.com/livepeer/stream-tester/model"
)

const saveDownloaded = false

func init() {
	rand.Seed(time.Now().UnixNano())
}

// streamer streams multiple RTMP streams into broadcaster node,
// reads back source and transcoded segments and count them
// and calculates success rate from these numbers
type streamer struct {
	ctx                 context.Context
	cancel              context.CancelFunc
	uploaders           []*rtmpStreamer
	downloaders         []*m3utester
	totalSegmentsToSend int
	stopSignal          bool
	wowzaMode           bool
	mistMode            bool
	mapi                *mist.API
	lapi                *livepeer.API
	createdMistStreams  []string
}

// NewStreamer returns new streamer
func NewStreamer(ctx context.Context, cancel context.CancelFunc, wowzaMode, mistMode bool, mapi *mist.API, lapi *livepeer.API) model.Streamer {
	return &streamer{
		ctx:       ctx,
		cancel:    cancel,
		wowzaMode: wowzaMode,
		mistMode:  mistMode,
		mapi:      mapi,
		lapi:      lapi,
	}
}

func (sr *streamer) Done() <-chan struct{} {
	return sr.ctx.Done()
}

func (sr *streamer) Cancel() {
	if sr.mapi != nil && len(sr.createdMistStreams) > 0 {
		err := sr.mapi.DeleteStreams(sr.createdMistStreams...)
		if err != nil {
			messenger.SendFatalMessage(fmt.Sprintf("Error deleting streams %+v from Mist", sr.createdMistStreams))
		}
		sr.createdMistStreams = nil
	}
	sr.cancel()
}

func (sr *streamer) Stop() {
	sr.stopSignal = true
	for _, up := range sr.uploaders {
		up.file.Close()
	}
}

func (sr *streamer) StartStreams(sourceFileName, bhost, rtmpPort, ohost, mediaPort string, simStreams, repeat uint, streamDuration time.Duration,
	notFinal, measureLatency, noBar bool, groupStartBy int, startDelayBetweenGroups, waitForTarget time.Duration) (string, error) {

	showProgress := !noBar
	var segments int
	glog.Infof("Counting segments in %s", sourceFileName)
	segments = GetNumberOfSegments(sourceFileName, streamDuration)
	glog.Infof("Counted %d source segments (for %s duration)", segments, streamDuration)

	nRtmpPort, err := strconv.Atoi(rtmpPort)
	if err != nil {
		return "", err
	}
	nMediaPort, err := strconv.Atoi(mediaPort)
	if err != nil {
		return "", err
	}

	var overallBar *uiprogress.Bar
	sr.totalSegmentsToSend = segments * int(simStreams) * int(repeat)
	if showProgress {
		uiprogress.Start()
		if repeat > 1 {
			overallBar = uiprogress.AddBar(sr.totalSegmentsToSend).AppendCompleted().PrependFunc(func(b *uiprogress.Bar) string {
				return "total: "
			})
			go func() {
				for {
					time.Sleep(5 * time.Second)
					st, _ := sr.Stats("")
					overallBar.Set(st.SentSegments)
				}
			}()
		}
	}

	sr.stopSignal = false
	baseManfistID := strings.ReplaceAll(path.Base(sourceFileName), ".", "") + "_" + randName()

	go func() {
		for i := 0; i < int(repeat); i++ {
			if repeat > 1 {
				glog.Infof("Starting %d streaming session", i)
			}
			err := sr.startStreams(baseManfistID, sourceFileName, i, bhost, ohost, nRtmpPort, nMediaPort, simStreams, showProgress, measureLatency,
				streamDuration, groupStartBy, startDelayBetweenGroups, waitForTarget)
			if err != nil {
				glog.Fatal(err)
				return
			}
			if sr.stopSignal {
				break
			}
		}
		// messenger.SendMessage(sr.AnalyzeFormatted(true))
		// fmt.Printf(sr.AnalyzeFormatted(false))
		if !notFinal {
			sr.cancel()
		}
	}()
	return baseManfistID, nil
}

func (sr *streamer) startStreams(baseManfistID, sourceFileName string, repeatNum int, bhost, mhost string, nRtmpPort, nMediaPort int, simStreams uint, showProgress,
	measureLatency bool, streamDuration time.Duration, groupStartBy int, startDelayBetweenGroups, waitForTarget time.Duration) error {

	// fmt.Printf("Starting streaming %s to %s:%d, number of streams is %d\n", sourceFileName, bhost, nRtmpPort, simStreams)
	msg := fmt.Sprintf("Starting streaming %s (repeat %d) to %s:%d, number of streams is %d, reading back from %s:%d\n", baseManfistID, repeatNum, bhost, nRtmpPort, simStreams,
		mhost, nMediaPort)
	messenger.SendMessage(msg)
	fmt.Println(msg)
	rtmpURLTemplate := "rtmp://%s:%d/%s"
	mediaURLTemplate := "http://%s:%d/stream/%s.m3u8"
	if sr.wowzaMode {
		rtmpURLTemplate = "rtmp://%s:%d/live/%s"
		mediaURLTemplate = "http://%s:%d/live/ngrp:%s_all/playlist.m3u8"
	}
	if sr.mistMode {
		rtmpURLTemplate = "rtmp://%s:%d/live/%s"
		mediaURLTemplate = "http://%s:%d/hls/%s/index.m3u8"
	}
	ctx, cancel := context.WithCancel(sr.ctx)

	var wg sync.WaitGroup
	started := make(chan interface{})
	go func() {
		for i := 0; i < int(simStreams); i++ {
			if groupStartBy > 0 && i%groupStartBy == 0 {
				startDelayBetweenGroups = 2*time.Second + time.Duration(rand.Intn(4000))*time.Millisecond
				glog.Infof("Waiting for %s before starting stream %d", startDelayBetweenGroups, i)
				if model.Production {
					time.Sleep(startDelayBetweenGroups)
				}
			}
			manifestID := fmt.Sprintf("%s_%d_%d", baseManfistID, repeatNum, i)
			if sr.mapi != nil {
				err := sr.mapi.CreateStream(manifestID, []string{"P720p30fps16x9"}, nil, "1", "", "", false)
				if err != nil {
					messenger.SendFatalMessage(fmt.Sprintf("Error creating stream %s on Mist", manifestID))
				}
				sr.createdMistStreams = append(sr.createdMistStreams, manifestID)
			}
			if sr.lapi != nil {
				sid, err := sr.lapi.CreateStream(manifestID)
				if err != nil {
					glog.Fatalf("Error creating stream using Livepeer API: %v", err)
					// return err
				}
				glog.V(model.SHORT).Infof("Create Livepeer stream %s", sid)
				manifestID = sid
			}
			rtmpURL := fmt.Sprintf(rtmpURLTemplate, bhost, nRtmpPort, manifestID)
			mediaURL := fmt.Sprintf(mediaURLTemplate, mhost, nMediaPort, manifestID)
			glog.Infof("RTMP: %s", rtmpURL)
			glog.Infof("MEDIA: %s", mediaURL)
			if sr.mistMode {
				time.Sleep(50 * time.Millisecond)
				messenger.SendMessage(mediaURL)
			}
			var bar *uiprogress.Bar
			if showProgress {
				/*
					bar = uiprogress.AddBar(totalSegments).AppendCompleted().PrependElapsed()
				*/
			}
			/*
				status, err := broadcaster.Status(fmt.Sprintf("http://%s:7935/status", bhost))
				if err != nil {
					glog.Fatal(err)
				}
				glog.Infof("Got this status: %+v", status)
			*/
			if false {
				// SaveNewStreams(ctx, bhost)
				SaveNewStreams(ctx, "localhost", "10.140.19.178", "10.140.21.136")
			}

			sctx, scancel := context.WithCancel(ctx)
			var sentTimesMap *utils.SyncedTimesMap
			var segmentsMatcher *segmentsMatcher
			if measureLatency {
				// sentTimesMap = utils.NewSyncedTimesMap()
				segmentsMatcher = newsementsMatcher()
			}
			up := newRtmpStreamer(sctx, scancel, rtmpURL, sourceFileName, baseManfistID, sentTimesMap, bar, sr.wowzaMode, segmentsMatcher)
			wg.Add(1)
			go func() {
				up.StartUpload(sourceFileName, rtmpURL, streamDuration, waitForTarget)
				wg.Done()
			}()
			sr.uploaders = append(sr.uploaders, up)
			down := newM3UTester(sctx, sentTimesMap, sr.wowzaMode, sr.mistMode, false, false, saveDownloaded, segmentsMatcher, nil, "")
			go findSkippedSegmentsNumber(up, down)
			sr.downloaders = append(sr.downloaders, down)
			down.Start(mediaURL)
			// put random delay before start of next stream
			// time.Sleep(time.Duration(rand.Intn(2)+2) * time.Second)
		}
		started <- nil
	}()
	<-started
	glog.Info("Streams started, waiting.")
	wg.Wait()
	glog.Info("RTMP upload done.")
	cancel()
	if sr.mapi != nil && len(sr.createdMistStreams) > 0 {
		glog.Infof("Waiting 8 seconds before delete stream from the Mist server")
		time.Sleep(8 * time.Second)
		err := sr.mapi.DeleteStreams(sr.createdMistStreams...)
		if err != nil {
			messenger.SendFatalMessage(fmt.Sprintf("Error deleting streams %+v from Mist", sr.createdMistStreams))
		}
		sr.createdMistStreams = nil
	}
	return nil
}

func findSkippedSegmentsNumber(rtmp *rtmpStreamer, mt *m3utester) {
	for {
		time.Sleep(3 * time.Second)
		if tm, ok := mt.GetFIrstSegmentTime(); ok {
			// check rtmp streamer
			// tm == 5
			// [2.5, 5,  7.5]
			for i, segTime := range rtmp.counter.segmentsStartTimes {
				if segTime > tm {
					break
				}
				if segTime >= tm {
					rtmp.skippedSegments = i + 1
					glog.V(model.VERBOSE).Infof("Found that %d segments was skipped, first segment time is %s, in rtmp %s",
						rtmp.skippedSegments, tm, segTime)
					return
				}
			}
			return
		}
	}
}

/*
func (sr *streamer) StatsFormatted() string {
	r := ""
	for _, md := range sr.downloaders {
		r += md.StatsFormatted()
	}
	return r
}
*/

/*
func (sr *streamer) DownStatsFormatted() string {
	res := ""
	for i, dl := range sr.downloaders {
		if len(sr.downloaders) > 1 {
			res += fmt.Sprintf("Downloads for stream %d\n", i)
		}
		res += dl.DownloadStatsFormatted()
	}
	return res
}
*/

/*
func (sr *streamer) AnalyzeFormatted(short bool) string {
	return ""
	// return sr.analyzeFormatted(short, true)
}
*/

/*
func (sr *streamer) analyzeFormatted(short, streamEnded bool) string {
	res := ""
	for i, dl := range sr.downloaders {
		if len(sr.downloaders) > 1 {
			res += fmt.Sprintf("Analisys for stream %d\n", i)
		}
		res += dl.AnalyzeFormatted(short)
	}
	stats := sr.Stats("")
	res += "Latencies: " + stats.TranscodedLatencies.String() + "\n"
	return res
}
*/

func (sr *streamer) Stats(basedManifestID string) (*model.Stats, error) {
	stats := &model.Stats{
		RTMPstreams:         len(sr.uploaders),
		MediaStreams:        len(sr.downloaders),
		TotalSegmentsToSend: sr.totalSegmentsToSend,
		Finished:            true,
		WowzaMode:           sr.wowzaMode,
	}
	sourceLatencies := utils.LatenciesCalculator{}
	transcodedLatencies := utils.LatenciesCalculator{}
	found := false
	for i, rs := range sr.uploaders {
		if basedManifestID != "" && rs.baseManifestID != basedManifestID {
			continue
		}
		found = true
		if stats.StartTime.IsZero() {
			stats.StartTime = rs.started
		} else if !rs.started.IsZero() && stats.StartTime.After(rs.started) {
			stats.StartTime = rs.started
		}
		// Broadcaster always skips at lest first segment, and potentially more
		stats.SentSegments += rs.counter.segments - rs.skippedSegments
		if rs.connectionLost {
			stats.ConnectionLost++
		}
		if rs.active {
			stats.RTMPActiveStreams++
			stats.Finished = false
		}
		stats.SentKeyFrames += rs.counter.keyFrames
		mt := sr.downloaders[i]
		ds := mt.stats()
		stats.DownloadedSegments += ds.success
		stats.FailedToDownloadSegments += ds.fail
		stats.BytesDownloaded += ds.bytes
		stats.Retries += ds.retries
		// stats.Gaps += ds.gaps
		stats.DownloadedKeyFrames += ds.keyframes
		if mt.segmentsMatcher != nil {
			for _, md := range mt.downloads {
				md.mu.Lock()
				lcp := make([]time.Duration, len(md.latencies), len(md.latencies))
				copy(lcp, md.latencies)
				if md.source {
					sourceLatencies.Add(lcp)
				} else {
					transcodedLatencies.Add(lcp)
					lcp := make([]time.Duration, len(md.latenciesPerStream))
					copy(lcp, md.latenciesPerStream)
					stats.RawTranscodeLatenciesPerStream = append(stats.RawTranscodeLatenciesPerStream, lcp)
				}
				md.mu.Unlock()
			}
		}
		mt.mu.RLock()
		// find skipped keyframes number
		skipSourceSegments := 0
		var sourceTimes []time.Duration
		for _, dlkey := range mt.downloadsKeys {
			dl := mt.downloads[dlkey]
			dl.mu.Lock()
			if dl.source {
				stats.DownloadedSourceSegments += dl.stats.success
			} else {
				stats.DownloadedTranscodedSegments += dl.stats.success
			}
			if dl.source && dl.firstSegmentParsed {
				glog.V(model.VERBOSE).Infof("===?> first time %s key ptss %+v start times %+v", dl.firstSegmentTime, rs.counter.keyFramesPTSs, rs.counter.segmentsStartTimes)
				// glog.Infof("==> keyFramesPTSs: %+v", rs.counter.keyFramesPTSs)
				for ki, pts := range rs.counter.keyFramesPTSs {
					if pts >= dl.firstSegmentTime || isTimeEqualM(pts, dl.firstSegmentTime) {
						glog.V(model.VERBOSE).Infof("===> ki %d pts %s fst %s before skipping %d", ki, pts, dl.firstSegmentTime, stats.SentKeyFrames)
						stats.SentKeyFrames -= ki
						break
					}
				}
				lastGotKeyframe := dl.lastKeyFramesPTSs[len(dl.lastKeyFramesPTSs)-1]
				var toSkip int
				for i := len(rs.counter.lastKeyFramesPTSs) - 1; i >= 0; i-- {
					lskf := rs.counter.lastKeyFramesPTSs[i]
					if lskf-lastGotKeyframe > 100*time.Millisecond {
						toSkip++
					} else {
						break
					}
				}
				glog.V(model.VERBOSE).Infof("==> lastGotKeyframe %s toSkip %d last sent key frames %+v last got key frames %+v", lastGotKeyframe, toSkip,
					rs.counter.lastKeyFramesPTSs, dl.lastKeyFramesPTSs)
				stats.SentKeyFrames -= toSkip
				sourceTimes = dl.firstSegmentTimes
			}
			if !dl.source && dl.firstSegmentParsed && sr.mistMode {
				glog.V(model.VERBOSE).Infof("===?> transcoded first time %s start times %+v", dl.firstSegmentTime, sourceTimes)
				for i, st := range sourceTimes {
					if absTimeTiff(st, dl.firstSegmentTime) < 2000*time.Millisecond {
						skipSourceSegments = i
						break
					}
				}
				glog.Infof("=====> skipping %d source segments", skipSourceSegments)
			}
			// glog.Infof("Downloaded segments for source=%v stream is %d:", dl.source, len(dl.downloadedSegments))
			// glog.Infoln("\n" + strings.Join(dl.downloadedSegments, "\n"))
			dl.mu.Unlock()
		}
		mt.mu.RUnlock()
		stats.DownloadedSourceSegments -= skipSourceSegments
		if sr.mistMode && stats.DownloadedSourceSegments > 0 {
			stats.DownloadedSourceSegments--
		}
	}
	if !found {
		return stats, model.ErroNotFound
	}
	// glog.Infof("=== source latencies: %+v", sourceLatencies)
	// glog.Infof("=== transcoded latencies: %+v", transcodedLatencies)
	sourceLatencies.Prepare()
	transcodedLatencies.Prepare()
	avg, p50, p95, p99 := sourceLatencies.Calc()
	stats.SourceLatencies = model.Latencies{Avg: avg, P50: p50, P95: p95, P99: p99}
	avg, p50, p95, p99 = transcodedLatencies.Calc()
	stats.TranscodedLatencies = model.Latencies{Avg: avg, P50: p50, P95: p95, P99: p99}
	if stats.SentSegments > 0 {
		stats.SuccessRate = float64(stats.DownloadedSegments) / ((float64(model.ProfilesNum) + 1) * float64(stats.SentSegments)) * 100
	}
	if stats.SentKeyFrames > 0 && stats.DownloadedSourceSegments > 0 {
		// stats.SuccessRate2 = float64(stats.DownloadedKeyFrames) / ((float64(model.ProfilesNum) + 1) * float64(stats.SentKeyFrames)) * 100
		stats.SuccessRate2 = float64(stats.DownloadedKeyFrames) / float64(stats.SentKeyFrames) * (float64(stats.DownloadedTranscodedSegments+stats.DownloadedSourceSegments) / float64(stats.DownloadedSourceSegments*(model.ProfilesNum+1))) * 100
		if sr.mistMode {
			stats.SuccessRate = stats.SuccessRate2
		}
	}
	stats.ShouldHaveDownloadedSegments = (model.ProfilesNum + 1) * stats.SentSegments
	stats.ProfilesNum = model.ProfilesNum
	stats.RawSourceLatencies = sourceLatencies.Raw()
	stats.RawTranscodedLatencies = transcodedLatencies.Raw()
	return stats, nil
}

func randName() string {
	x := make([]byte, 10, 10)
	for i := 0; i < len(x); i++ {
		x[i] = byte(rand.Uint32())
	}
	return fmt.Sprintf("%x", x)
}
