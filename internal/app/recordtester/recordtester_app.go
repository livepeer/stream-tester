package recordtester

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"time"

	"github.com/golang/glog"
	livepeerAPI "github.com/livepeer/go-api-client"
	"github.com/livepeer/joy4/format/mp4"
	"github.com/livepeer/joy4/format/mp4/mp4io"
	"github.com/livepeer/stream-tester/apis/livepeer"
	"github.com/livepeer/stream-tester/internal/testers"
	"github.com/livepeer/stream-tester/messenger"
	"github.com/livepeer/stream-tester/model"
)

type (
	// IRecordTester ...
	IRecordTester interface {
		// Start start test. Blocks until finished.
		Start(fileName string, testDuration, pauseDuration time.Duration) (int, error)
		Cancel()
		Done() <-chan struct{}
		VODStats() model.VODStats
		Clean()
		StreamID() string
		Stream() *livepeerAPI.CreateStreamResp
	}

	RecordTesterOptions struct {
		*livepeerAPI.Client
		Analyzers           testers.AnalyzerByRegion
		Ingest              *livepeerAPI.Ingest
		RecordObjectStoreId string
		UseForceURL         bool
		UseHTTP             bool
		TestMP4             bool
		TestStreamHealth    bool
	}

	recordTester struct {
		ctx                 context.Context
		cancel              context.CancelFunc
		lapi                *livepeerAPI.Client
		lanalyzers          testers.AnalyzerByRegion
		ingest              *livepeerAPI.Ingest
		recordObjectStoreId string
		useForceURL         bool
		useHTTP             bool
		mp4                 bool
		streamHealth        bool

		// mutable fields
		streamID string
		stream   *livepeerAPI.CreateStreamResp
		vodStats model.VODStats
	}
)

// NewRecordTester ...
func NewRecordTester(gctx context.Context, opts RecordTesterOptions) IRecordTester {
	ctx, cancel := context.WithCancel(gctx)
	rt := &recordTester{
		lapi:                opts.Client,
		lanalyzers:          opts.Analyzers,
		ingest:              opts.Ingest,
		ctx:                 ctx,
		cancel:              cancel,
		recordObjectStoreId: opts.RecordObjectStoreId,
		useForceURL:         opts.UseForceURL,
		useHTTP:             opts.UseHTTP,
		mp4:                 opts.TestMP4,
		streamHealth:        opts.TestStreamHealth,
	}
	return rt
}

func (rt *recordTester) Start(fileName string, testDuration, pauseDuration time.Duration) (int, error) {
	defer rt.cancel()
	var err error
	var broadcasters []string
	ingest, err := rt.getIngestInfo()
	if err != nil {
		return 255, err
	}
	apiTry := 0
	for {
		broadcasters, err = rt.lapi.Broadcasters()
		if err != nil {
			if testers.Timedout(err) && apiTry < 3 {
				apiTry++
				continue
			}
			// exit(255, fileName, *fileArg, err)
			return 255, err
		}
		break
	}
	apiTry = 0
	glog.Infof("Got broadcasters: %+v", broadcasters)
	fmt.Printf("Streaming video file '%s'\n", fileName)
	/*
		httpIngestURLTemplates := make([]string, 0, len(broadcasters))
		for _, b := range broadcasters {
			httpIngestURLTemplates = append(httpIngestURLTemplates, fmt.Sprintf("%s/live/%%s", b))
		}
	*/
	if rt.useHTTP && len(broadcasters) == 0 {
		// exit(254, fileName, *fileArg, errors.New("Empty list of broadcasters"))
		return 254, errors.New("empty list of broadcasters")
	} else if (!rt.useHTTP && ingest.Ingest == "") || ingest.Playback == "" {
		return 254, errors.New("empty ingest URLs")
		// exit(254, fileName, *fileArg, errors.New("Empty list of ingests"))
	}
	// glog.Infof("All cool!")
	hostName, _ := os.Hostname()
	streamName := fmt.Sprintf("%s_%s", hostName, time.Now().Format("2006-01-02T15:04:05Z07:00"))
	var stream *livepeerAPI.CreateStreamResp
	for {
		stream, err = rt.lapi.CreateStreamEx2(streamName, true, rt.recordObjectStoreId, "", nil, livepeerAPI.StandardProfiles...)
		if err != nil {
			if testers.Timedout(err) && apiTry < 3 {
				apiTry++
				continue
			}
			glog.Errorf("Error creating stream using Livepeer API: %v", err)
			// exit(253, fileName, *fileArg, err)
			return 253, err
		}
		break
	}
	apiTry = 0
	rt.streamID = stream.ID
	rt.stream = stream
	messenger.SendMessage(fmt.Sprintf(":information_source: Created stream id=%s", stream.ID))
	// createdAPIStreams = append(createdAPIStreams, stream.ID)
	glog.V(model.VERBOSE).Infof("Created Livepeer stream id=%s streamKey=%s playbackId=%s name=%s", stream.ID, stream.StreamKey, stream.PlaybackID, streamName)
	// glog.Infof("Waiting 5 second for stream info to propagate to the Postgres replica")
	// time.Sleep(5 * time.Second)
	rtmpURL := fmt.Sprintf("%s/%s", ingest.Ingest, stream.StreamKey)
	// rtmpURL = fmt.Sprintf("%s/%s", ingests[0].Ingest, stream.ID)

	testerFuncs := []testers.StartTestFunc{}
	if rt.streamHealth {
		testerFuncs = append(testerFuncs, func(ctx context.Context, mediaURL string, waitForTarget time.Duration, opts testers.Streamer2Options) testers.Finite {
			return testers.NewStreamHealth(ctx, stream.ID, rt.lanalyzers, waitForTarget)
		})
	}

	mediaURL := fmt.Sprintf("%s/%s/index.m3u8", ingest.Playback, stream.PlaybackID)
	glog.V(model.SHORT).Infof("RTMP: %s", rtmpURL)
	glog.V(model.SHORT).Infof("MEDIA: %s", mediaURL)
	if rt.useHTTP {
		sterr := rt.doOneHTTPStream(fileName, streamName, broadcasters[0], testDuration, stream)
		if sterr != nil {
			glog.Warningf("Streaming returned error err=%v", sterr)
			return 3, err
		}
		if pauseDuration > 0 {
			glog.Infof("Pause specified, waiting %s before streaming second time", pauseDuration)
			time.Sleep(pauseDuration)
			sterr = rt.doOneHTTPStream(fileName, streamName, broadcasters[0], testDuration, stream)
			if sterr != nil {
				glog.Warningf("Second time streaming returned error err=%v", sterr)
				return 3, err
			}
			testDuration *= 2
		}
	} else {

		sr2 := testers.NewStreamer2(rt.ctx, testers.Streamer2Options{}, testerFuncs...)
		sr2.StartStreaming(fileName, rtmpURL, mediaURL, 30*time.Second, testDuration)
		// <-sr2.Done()
		srerr := sr2.Err()
		glog.Infof("Streaming stream id=%s done err=%v", stream.ID, srerr)
		var re *testers.RTMPError
		if errors.As(srerr, &re) {
			return 2, re
		}
		if srerr != nil {
			glog.Warningf("Streaming returned error err=%v", srerr)
			return 3, err
		}
		stats, err := sr2.Stats()
		if err != nil {
			glog.Warningf("Stats returned error err=%v", err)
			return 21, err
		}
		glog.Infof("Streaming success rate=%v", stats.SuccessRate)
		if err = rt.isCancelled(); err != nil {
			return 0, err
		}
		if pauseDuration > 0 {
			glog.Infof("Pause specified, waiting %s before streaming second time", pauseDuration)
			time.Sleep(pauseDuration)
			sr2 := testers.NewStreamer2(rt.ctx, testers.Streamer2Options{}, testerFuncs...)
			go sr2.StartStreaming(fileName, rtmpURL, mediaURL, 30*time.Second, testDuration)
			<-sr2.Done()
			srerr := sr2.Err()
			glog.Infof("Streaming second stream id=%s done", stream.ID)
			var re *testers.RTMPError
			if errors.As(srerr, &re) {
				return 2, re
			}
			if srerr != nil {
				glog.Warningf("Streaming second returned error err=%v", srerr)
				return 3, err
			}
			stats, err := sr2.Stats()
			if err != nil {
				glog.Warningf("Stats returned error err=%v", err)
				return 21, err
			}
			glog.Infof("Streaming second time success rate=%v", stats.SuccessRate)
			if err = rt.isCancelled(); err != nil {
				return 0, err
			}
			testDuration *= 2
		}
	}
	if err := rt.isCancelled(); err != nil {
		return 0, err
	}
	glog.Infof("Waiting 10 seconds")
	time.Sleep(10 * time.Second)
	// now get sessions
	sessions, err := rt.lapi.GetSessionsNewR(stream.ID, false)
	if err != nil {
		glog.Errorf("Error getting sessions for stream id=%s err=%v", stream.ID, err)
		// exit(252, fileName, *fileArg, err)
		return 252, err
	}
	glog.Infof("Sessions: %+v", sessions)
	if len(sessions) != 1 {
		err := fmt.Errorf("should have one session, got %d", len(sessions))
		glog.Error(err)
		// exit(251, fileName, *fileArg, err)
		return 251, err
	}
	sess := sessions[0]
	if len(sess.Profiles) != len(stream.Profiles) {
		glog.Infof("session: %+v", sess)
		err := fmt.Errorf("got %d profiles but should have %d", len(sess.Profiles), len(stream.Profiles))
		return 251, err
		// exit(251, fileName, *fileArg, err)
	}
	if sess.RecordingStatus != livepeer.RecordingStatusWaiting {
		err := fmt.Errorf("recording status is %s but should be %s", sess.RecordingStatus, livepeer.RecordingStatusWaiting)
		return 250, err
		// exit(250, fileName, *fileArg, err)
	}
	if err = rt.isCancelled(); err != nil {
		return 0, err
	}

	glog.Info("Streaming done, waiting for recording URL to appear")
	if rt.useForceURL {
		time.Sleep(5 * time.Second)
	} else {
		time.Sleep(6*time.Minute + 20*time.Second)
	}
	if err = rt.isCancelled(); err != nil {
		return 0, err
	}

	sessions, err = rt.lapi.GetSessionsNewR(stream.ID, rt.useForceURL)
	if err != nil {
		err := fmt.Errorf("error getting sessions for stream id=%s err=%v", stream.ID, err)
		return 252, err
		// exit(252, fileName, *fileArg, err)
	}
	glog.Infof("Sessions: %+v", sessions)
	if err = rt.isCancelled(); err != nil {
		return 0, err
	}

	sess = sessions[0]
	statusShould := livepeer.RecordingStatusReady
	if rt.useForceURL {
		statusShould = livepeer.RecordingStatusWaiting
	}
	if sess.RecordingStatus != statusShould {
		err := fmt.Errorf("recording status is %s but should be %s", sess.RecordingStatus, statusShould)
		return 240, err
		// exit(250, fileName, *fileArg, err)
	}
	if sess.RecordingURL == "" {
		err := fmt.Errorf("recording URL should appear by now")
		return 249, err
		// exit(249, fileName, *fileArg, err)
	}
	glog.Infof("recordingURL=%s downloading now", sess.RecordingURL)

	// started := time.Now()
	// downloader := testers.NewM3utester2(gctx, sess.RecordingURL, false, false, false, false, 5*time.Second, nil)
	// <-downloader.Done()
	// glog.Infof(`Pulling stopped after %s`, time.Since(started))
	// exit(55, fileName, *fileArg, err)

	glog.Info("Record testing done, waiting for Asset to be available")
	if err = rt.isCancelled(); err != nil {
		return 0, err
	}

	assetId := ""
	wait_interval := 30 * time.Second
	elapsed_time := 0 * time.Second
asset:
	for {
		glog.Infof("Waiting %s for asset, elapsed=%s", wait_interval, elapsed_time)
		time.Sleep(wait_interval)
		elapsed_time = elapsed_time + wait_interval
		if err = rt.isCancelled(); err != nil {
			return 0, err
		}

		assets, err := rt.lapi.ListAssets() // assets are retrieved in order of most recent first
		if err != nil {
			glog.Errorf("Error listing assets err=%v", err)
			return 241, err
			// exit(241, fileName, *fileArg, err)
		}
		for _, asset := range *assets { // TODO: only need to check first n assets due to ordering
			if asset.Name == fmt.Sprintf("live-to-vod-%s", sess.ID) {
				assetId = asset.ID
				break asset
			}
		}

		// TODO: timeout required here?
	}

	task_name := fmt.Sprintf("live-to-vod-transcode-%s", sess.ID)
	createTranscodeTaskResp, _ := rt.lapi.CreateTranscodeTask(assetId, task_name, livepeerAPI.StandardProfiles[0])
	if err != nil {
		glog.Errorf("Error creating transcode task err=%v", err)
		return 242, err
		// exit(242, fileName, *fileArg, err)
	}
	transcodeTask := createTranscodeTaskResp.Task
	transcodeAsset := createTranscodeTaskResp.Asset
	glog.Infof("Asset is available id=%s, transcoding taskId=%s outputAssetId=%s", assetId, transcodeTask.ID, transcodeAsset.ID)

	wait_interval = 15 * time.Second
	elapsed_time = 0 * time.Second
	for {
		glog.Infof("Waiting %s for transcode output asset id=%s, elapsed=%s", wait_interval, transcodeAsset.ID, elapsed_time)
		time.Sleep(wait_interval)
		elapsed_time = elapsed_time + wait_interval
		if err = rt.isCancelled(); err != nil {
			return 0, err
		}

		asset, err := rt.lapi.GetAsset(transcodeAsset.ID)
		if err != nil {
			glog.Errorf("Error retrieving asset id=%s err=%v", transcodeAsset.ID, err)
			return 243, err
			// exit(243, fileName, *fileArg, err)
		}
		if asset.Status == "ready" {
			break
		}
		if asset.Status != "waiting" {
			glog.Errorf("Error transcoding asset id=%s, task id=%s outputAssetId=%s err=%v", assetId, transcodeTask.ID, transcodeAsset.ID, err)
			return 244, err
			// exit(244, fileName, *fileArg, err)
		}

		// TODO: timeout required here?
	}

	createExportTaskResp, _ := rt.lapi.CreateExportTask(transcodeAsset.ID)
	if err != nil {
		glog.Errorf("Error creating export task err=%v", err)
		return 245, err
		// exit(245, fileName, *fileArg, err)
	}
	exportTask := createExportTaskResp.Task
	glog.Infof("Transcode output asset id=%s ready, exporting, taskId=%s", transcodeAsset.ID, exportTask.ID)

	wait_interval = 5 * time.Second
	elapsed_time = 0 * time.Second
	for {
		glog.Infof("Waiting %s for asset id=%s to be exported, elapsed=%s", wait_interval, transcodeAsset.ID, elapsed_time)
		time.Sleep(wait_interval)
		elapsed_time = elapsed_time + wait_interval
		if err = rt.isCancelled(); err != nil {
			return 0, err
		}

		task, err := rt.lapi.GetTask(exportTask.ID)
		if err != nil {
			glog.Errorf("Error retrieving task id=%s err=%v", exportTask.ID, err)
			return 246, err
			// exit(246, fileName, *fileArg, err)
		}
		if task.Output.Export.IPFS.VideoFileGatewayUrl != "" {
			glog.Infof("Export success, task id=%s ipfs link=%s", exportTask.ID, task.Output.Export.IPFS.VideoFileGatewayUrl)
			break
		}
		// TODO: timeout required here?
	}

	glog.Info("Done")
	// lapi.DeleteStream(stream.ID)
	// exit(0, fileName, *fileArg, err)
	if err = rt.isCancelled(); err != nil {
		return 0, err
	}
	if rt.mp4 {
		es, err := rt.checkDownMp4(stream, sess.Mp4Url, testDuration, pauseDuration > 0)
		if err != nil {
			return es, err
		}
	}

	es, err := rt.checkDown(stream, sess.RecordingURL, testDuration, pauseDuration > 0)
	if es == 0 {
		rt.lapi.DeleteStream(stream.ID)
		// exit(0, fileName, *fileArg, err)
	}

	// uploader := testers.NewRtmpStreamer(gctx, rtmpURL)
	// uploader.StartUpload(fileName, rtmpURL, -1, 30*time.Second)
	return es, err
}

func (rt *recordTester) getIngestInfo() (*livepeerAPI.Ingest, error) {
	if rt.ingest != nil {
		return rt.ingest, nil
	}
	var ingests []livepeerAPI.Ingest
	apiTry := 0
	for {
		var err error
		ingests, err = rt.lapi.Ingest(false)
		if err != nil {
			if testers.Timedout(err) && apiTry < 3 {
				apiTry++
				continue
			}
			return nil, err
		}
		break
	}
	glog.Infof("Got ingests: %+v", ingests)
	if len(ingests) == 0 {
		return nil, errors.New("empty list of ingests")
	}
	return &ingests[0], nil
}

func (rt *recordTester) doOneHTTPStream(fileName, streamName, broadcasterURL string, testDuration time.Duration, stream *livepeerAPI.CreateStreamResp) error {
	var session *livepeerAPI.CreateStreamResp
	var err error
	apiTry := 0
	for {
		session, err = rt.lapi.CreateStreamEx2(streamName, true, rt.recordObjectStoreId, stream.ID, nil, livepeerAPI.StandardProfiles...)
		if err != nil {
			if testers.Timedout(err) && apiTry < 3 {
				apiTry++
				continue
			}
			glog.Errorf("Error creating stream session using Livepeer API: %v", err)
			// exit(253, fileName, *fileArg, err)
			return err
		}
		break
	}
	hs := testers.NewHTTPStreamer(rt.ctx, false, "not used")
	httpIngestBaseURL := fmt.Sprintf("%s/live/%s", broadcasterURL, session.ID)
	glog.Infof("httpIngestBaseURL=%s", httpIngestBaseURL)
	hs.StartUpload(fileName, httpIngestBaseURL, stream.ID, -1, -1, testDuration, 0)
	// <-hs.Done()
	stats, err := hs.Stats()
	glog.Infof("Streaming stream id=%s done err=%v", stream.ID, err)
	glog.Infof("Stats: %+v", stats)
	return err
}

func (rt *recordTester) isCancelled() error {
	select {
	case <-rt.ctx.Done():
		return context.Canceled
	default:
	}
	return nil
}

func (rt *recordTester) checkDownMp4(stream *livepeerAPI.CreateStreamResp, url string, streamDuration time.Duration, doubled bool) (int, error) {
	es := 0
	started := time.Now()
	glog.V(model.VERBOSE).Infof("Downloading mp4 url=%s stream id=%s", url, stream.ID)

	resp, err := http.Get(url)
	if err != nil {
		glog.Warningf("Error downloading mp4 for manifestID=%s url=%s", stream.ID, url)
		return 3, err
	}
	if resp.StatusCode != http.StatusOK {
		glog.Warningf("HTTP error downloading mp4 for manifestID=%s url=%s status=%s", stream.ID, url, resp.Status)
		return 3, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.Warningf("HTTP error downloading mp4 for manifestID=%s url=%s err=%v", stream.ID, url, err)
		return 3, err
	}
	glog.Infof("Downloaded bytes=%b manifestID=%s url=%s took=%s", len(body), stream.ID, url, time.Since(started))
	bodyR := bytes.NewReader(body)
	dem := mp4.NewDemuxer(bodyR)
	streams, err := dem.Streams()
	if err != nil {
		glog.Warningf("Error parsing mp4 for manifestID=%s url=%s err=%v", stream.ID, url, err)
		return 203, err
	}
	glog.Infof("Got %d streams in mp4 file manifestID=%s url=%s", len(streams), stream.ID, url)
	dur, err := calcMP4FileDuration(body)
	if err != nil {
		glog.Warningf("Error parsing mp4 for manifestID=%s url=%s err=%v", stream.ID, url, err)
		return 203, err
	}
	durDiffShould := 2 * time.Second
	if doubled {
		durDiffShould *= durDiffShould
	}
	durDiff := streamDuration - dur
	if durDiff < 0 {
		durDiff = -durDiff
	}
	if durDiff > durDiffShould {
		ers := fmt.Errorf("duration of mp4 differ by %s (got %s, should %s)", durDiff, dur, streamDuration)
		glog.Error(ers)
		return 300, err
	}
	return es, nil
}

func (rt *recordTester) checkDown(stream *livepeerAPI.CreateStreamResp, url string, streamDuration time.Duration, doubled bool) (int, error) {
	es := 0
	started := time.Now()
	downloader := testers.NewM3utester2(rt.ctx, url, false, false, false, false, 5*time.Second, nil, false)
	<-downloader.Done()
	glog.Infof(`Pulling for %s (%s) stopped after %s`, stream.ID, stream.PlaybackID, time.Since(started))
	if err := rt.isCancelled(); err != nil {
		return 0, err
	}
	vs := downloader.VODStats()
	rt.vodStats = vs
	if len(vs.SegmentsNum) != len(livepeerAPI.StandardProfiles)+1 {
		glog.Warningf("Number of renditions doesn't match! Has %d should %d", len(vs.SegmentsNum), len(livepeerAPI.StandardProfiles)+1)
		es = 35
	}
	glog.Infof("Stats for %s: %s", stream.ID, vs.String())
	glog.Infof("Stats for %s raw: %+v", stream.ID, vs)
	if ok, ers := vs.IsOk(streamDuration, doubled); !ok {
		glog.Warningf("NOT OK! (%s)", ers)
		es = 36
		return es, errors.New(ers)
	} else {
		glog.Infoln("All ok!")
	}
	return es, nil
}

func (rt *recordTester) Cancel() {
	rt.cancel()
}

func (rt *recordTester) Done() <-chan struct{} {
	return rt.ctx.Done()
}

func (rt *recordTester) VODStats() model.VODStats {
	return rt.vodStats
}

func (rt *recordTester) Clean() {
	if rt.streamID != "" {
		rt.lapi.DeleteStream(rt.streamID)
	}
}

func (rt *recordTester) StreamID() string {
	return rt.streamID
}

func (rt *recordTester) Stream() *livepeerAPI.CreateStreamResp {
	return rt.stream
}

type visitor = func(mp4io.Atom) bool

func walker(atoms []mp4io.Atom, visitor visitor) bool {
	res := false
	for _, atom := range atoms {
		if res = visitor(atom); res {
			break
		}
		if res = walker(atom.Children(), visitor); res {
			break
		}
	}
	return res
}

func calcFileDuration(atoms []mp4io.Atom) time.Duration {
	var timeScale int32
	var duration, fragmentBaseTime float64
	var durationDuration, fragmentBaseTimeDuration time.Duration
	walker(atoms, func(atom mp4io.Atom) bool {
		if atom.Tag() == mp4io.MVHD {
			mvhd := atom.(*mp4io.MovieHeader)
			durationDuration = time.Duration(mvhd.Duration) * time.Second / time.Duration(mvhd.TimeScale)
			duration = float64(mvhd.Duration) / float64(mvhd.TimeScale)
			fmt.Printf("MVHD duration %d time scale %d (%fsec) dur %s (%d)\n", mvhd.Duration, mvhd.TimeScale, duration, durationDuration, int64(durationDuration))
		}
		if atom.Tag() == mp4io.MDHD {
			mvhd := atom.(*mp4io.MediaHeader)
			timeScale = mvhd.TimeScale
			fmt.Printf("MDHD duration %d time scale %d (%fsec)\n", mvhd.Duration, mvhd.TimeScale, float64(mvhd.Duration)/float64(mvhd.TimeScale))
			duration = float64(mvhd.Duration) / float64(mvhd.TimeScale)
		}
		if atom.Tag() == mp4io.TFDT {
			mvhd := atom.(*mp4io.TrackFragDecodeTime)
			fragmentBaseTime = float64(mvhd.BaseMediaDecodeTime) / float64(timeScale)
			fragmentBaseTimeDuration = time.Duration(mvhd.BaseMediaDecodeTime) * time.Second / time.Duration(timeScale)
			fmt.Printf("TFDT base time %d time scale %d (%fsec) %s (%d)\n", mvhd.BaseMediaDecodeTime, timeScale, fragmentBaseTime,
				fragmentBaseTimeDuration, int64(fragmentBaseTimeDuration))
		}
		return false
	})
	fmt.Printf("Header duration %f fragment base time %f total duration %fsec\n", duration, fragmentBaseTime, duration+fragmentBaseTime)
	return durationDuration + fragmentBaseTimeDuration
}

func calcMP4FileDuration(data []byte) (time.Duration, error) {
	dataR := bytes.NewReader(data)
	atoms, err := mp4io.ReadFileAtoms(dataR)
	if err != nil {
		return 0, err
	}
	dur := calcFileDuration(atoms)
	return dur, nil
}
