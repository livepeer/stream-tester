package recordtester

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"time"

	"github.com/golang/glog"
	serfClient "github.com/hashicorp/serf/client"
	api "github.com/livepeer/go-api-client"
	"github.com/livepeer/joy4/format/mp4"
	"github.com/livepeer/joy4/format/mp4/mp4io"
	"github.com/livepeer/stream-tester/internal/testers"
	"github.com/livepeer/stream-tester/messenger"
	"github.com/livepeer/stream-tester/model"
)

type (
	// IRecordTester ...
	IRecordTester interface {
		// Start test. Blocks until finished.
		Start(fileName string, testDuration, pauseDuration time.Duration) (int, error)
		Cancel()
		Done() <-chan struct{}
		VODStats() model.VODStats
		Clean()
		StreamID() string
		Stream() *api.Stream
	}

	SerfOptions struct {
		UseSerf          bool
		SerfMembers      []serfClient.Member
		RandomSerfMember bool

		// TODO: pull from multiple nodes; each one will need to
		// trigger a separate playback
		SerfPullCount int
	}

	RecordTesterOptions struct {
		API                 *api.Client
		Analyzers           testers.AnalyzerByRegion
		Ingest              *api.Ingest
		RecordObjectStoreId string
		UseForceURL         bool
		RecordingWaitTime   time.Duration
		UseHTTP             bool
		TestMP4             bool
		TestStreamHealth    bool
	}

	recordTester struct {
		ctx                 context.Context
		cancel              context.CancelFunc
		lapi                *api.Client
		lanalyzers          testers.AnalyzerByRegion
		ingest              *api.Ingest
		recordObjectStoreId string
		useForceURL         bool
		recordingWaitTime   time.Duration
		useHTTP             bool
		mp4                 bool
		streamHealth        bool
		serfOpts            SerfOptions

		// mutable fields
		streamID string
		stream   *api.Stream
		vodStats model.VODStats
	}
)

// NewRecordTester ...
func NewRecordTester(gctx context.Context, opts RecordTesterOptions, serfOpts SerfOptions) IRecordTester {
	ctx, cancel := context.WithCancel(gctx)
	rt := &recordTester{
		lapi:                opts.API,
		lanalyzers:          opts.Analyzers,
		ingest:              opts.Ingest,
		ctx:                 ctx,
		cancel:              cancel,
		recordObjectStoreId: opts.RecordObjectStoreId,
		useForceURL:         opts.UseForceURL,
		recordingWaitTime:   opts.RecordingWaitTime,
		useHTTP:             opts.UseHTTP,
		mp4:                 opts.TestMP4,
		streamHealth:        opts.TestStreamHealth,
		serfOpts:            serfOpts,
	}
	return rt
}

func (rt *recordTester) Start(fileName string, testDuration, pauseDuration time.Duration) (i int, err error) {
	defer rt.cancel()
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
			return 255, err
		}
		break
	}
	apiTry = 0
	glog.V(model.DEBUG).Infof("Got broadcasters: %+v", broadcasters)
	glog.V(model.DEBUG).Infof("Streaming video file '%s'\n", fileName)

	if rt.useHTTP && len(broadcasters) == 0 {
		return 254, errors.New("empty list of broadcasters")
	} else if (!rt.useHTTP && ingest.Ingest == "") || ingest.Playback == "" {
		return 254, errors.New("empty ingest URLs")
	}

	hostName, _ := os.Hostname()
	streamName := fmt.Sprintf("%s_%s", hostName, time.Now().Format("2006-01-02T15:04:05Z07:00"))
	var stream *api.Stream
	for {
		stream, err = rt.lapi.CreateStream(api.CreateStreamReq{Name: streamName, Record: true, RecordObjectStoreId: rt.recordObjectStoreId})
		if err != nil {
			if testers.Timedout(err) && apiTry < 3 {
				apiTry++
				continue
			}
			glog.Errorf("Error creating stream using Livepeer API: %v", err)
			return 253, err
		}
		break
	}
	// Make sure we include the Stream and Playback IDs on any error message
	defer func() {
		if err != nil {
			err = fmt.Errorf("playbackId=%s %w", stream.PlaybackID, err)
		}
	}()
	apiTry = 0
	rt.streamID = stream.ID
	rt.stream = stream
	messenger.SendMessage(fmt.Sprintf(":information_source: Created stream id=%s", stream.ID))
	glog.V(model.VERBOSE).Infof("Created Livepeer stream streamId=%s streamKey=%s playbackId=%s name=%s", stream.ID, stream.StreamKey, stream.PlaybackID, streamName)
	rtmpURL := fmt.Sprintf("%s/%s", ingest.Ingest, stream.StreamKey)

	testerFuncs := []testers.StartTestFunc{}
	if rt.streamHealth {
		testerFuncs = append(testerFuncs, func(ctx context.Context, mediaURL string, waitForTarget time.Duration, opts testers.Streamer2Options) testers.Finite {
			return testers.NewStreamHealth(ctx, stream.ID, rt.lanalyzers, 2*time.Minute)
		})
	}

	// when pauseDuration is set, we will stream the same file twice sleeping for
	// the specified duration in between each.
	streamTwice := pauseDuration > 0

	mediaURL := fmt.Sprintf("%s/%s/index.m3u8", ingest.Playback, stream.PlaybackID)
	if rt.serfOpts.UseSerf {
		index := 0
		if rt.serfOpts.RandomSerfMember {
			index = rand.Intn(len(rt.serfOpts.SerfMembers))
		}
		serfMember := rt.serfOpts.SerfMembers[index]
		mediaURL = fmt.Sprintf("%s/hls/%s/index.m3u8", serfMember.Tags["https"], stream.PlaybackID)
	}
	glog.V(model.SHORT).Infof("RTMP: %s streamId=%s playbackId=%s", rtmpURL, stream.ID, stream.PlaybackID)
	glog.V(model.SHORT).Infof("MEDIA: %s streamId=%s playbackId=%s", mediaURL, stream.ID, stream.PlaybackID)
	if rt.useHTTP {
		sterr := rt.doOneHTTPStream(fileName, streamName, broadcasters[0], testDuration, stream)
		if sterr != nil {
			glog.Warningf("Streaming returned error err=%v streamId=%s playbackId=%s", sterr, stream.ID, stream.PlaybackID)
			return 3, err
		}
		if streamTwice {
			glog.Infof("Pause specified, waiting %s before streaming second time. streamId=%s playbackId=%s", pauseDuration, stream.ID, stream.PlaybackID)
			time.Sleep(pauseDuration)
			sterr = rt.doOneHTTPStream(fileName, streamName, broadcasters[0], testDuration, stream)
			if sterr != nil {
				glog.Warningf("Second time streaming returned error err=%v streamId=%s playbackId=%s", sterr, stream.ID, stream.PlaybackID)
				return 3, err
			}
		}
	} else {

		sr2 := testers.NewStreamer2(rt.ctx, testers.Streamer2Options{MistMode: true}, testerFuncs...)
		sr2.StartStreaming(fileName, rtmpURL, mediaURL, 2*time.Minute, testDuration)
		srerr := sr2.Err()

		glog.Infof("Streaming done err=%v streamId=%s playbackId=%s", srerr, stream.ID, stream.PlaybackID)
		var re *testers.RTMPError
		if errors.As(srerr, &re) {
			return 2, re
		}
		if srerr != nil {
			glog.Warningf("Streaming returned error err=%v streamId=%s playbackId=%s", srerr, stream.ID, stream.PlaybackID)
			return 3, srerr
		}
		stats, err := sr2.Stats()
		if err != nil {
			glog.Warningf("Stats returned error err=%v streamId=%s playbackId=%s", err, stream.ID, stream.PlaybackID)
			return 21, err
		}
		glog.Infof("Streaming success rate=%v streamId=%s playbackId=%s", stats.SuccessRate, stream.ID, stream.PlaybackID)
		if err = rt.isCancelled(); err != nil {
			return 0, err
		}
		if streamTwice {
			glog.Infof("Pause specified, waiting %s before streaming second time. streamId=%s playbackId=%s", pauseDuration, stream.ID, stream.PlaybackID)
			time.Sleep(pauseDuration)
			sr2 := testers.NewStreamer2(rt.ctx, testers.Streamer2Options{MistMode: true}, testerFuncs...)
			go sr2.StartStreaming(fileName, rtmpURL, mediaURL, 30*time.Second, testDuration)
			<-sr2.Done()
			srerr := sr2.Err()
			glog.Infof("Streaming second done. streamId=%s playbackId=%s", stream.ID, stream.PlaybackID)
			var re *testers.RTMPError
			if errors.As(srerr, &re) {
				return 2, re
			}
			if srerr != nil {
				glog.Warningf("Streaming second returned error err=%v streamId=%s playbackId=%s", srerr, stream.ID, stream.PlaybackID)
				return 3, srerr
			}
			stats, err := sr2.Stats()
			if err != nil {
				glog.Warningf("Stats returned error err=%v streamId=%s playbackId=%s", err, stream.ID, stream.PlaybackID)
				return 21, err
			}
			glog.Infof("Streaming second time success rate=%v streamId=%s playbackId=%s", stats.SuccessRate, stream.ID, stream.PlaybackID)
			if err = rt.isCancelled(); err != nil {
				return 0, err
			}
		}
	}
	if err := rt.isCancelled(); err != nil {
		return 0, err
	}
	glog.Infof("Waiting 10 seconds. streamId=%s playbackId=%s", stream.ID, stream.PlaybackID)
	time.Sleep(10 * time.Second)
	// now get sessions
	sessions, err := rt.lapi.GetSessionsNew(stream.ID, false)
	if err != nil {
		glog.Errorf("Error getting sessions err=%v streamId=%s playbackId=%s", err, stream.ID, stream.PlaybackID)
		return 252, err
	}
	glog.V(model.DEBUG).Infof("Sessions: %+v streamId=%s playbackId=%s", sessions, stream.ID, stream.PlaybackID)

	expectedSessions := 1
	if streamTwice {
		expectedSessions = 2
	}

	if len(sessions) != expectedSessions {
		err := fmt.Errorf("invalid session count, expected %d but got %d",
			expectedSessions, len(sessions))
		glog.Error(err)
		return 251, err
	}

	sess := sessions[0]
	if len(sess.Profiles) != len(stream.Profiles) {
		glog.Infof("session: %+v streamId=%s playbackId=%s", sess, stream.ID, stream.PlaybackID)
		err := fmt.Errorf("got %d profiles but should have %d", len(sess.Profiles), len(stream.Profiles))
		return 251, err
	}
	if sess.RecordingStatus != api.RecordingStatusWaiting {
		err := fmt.Errorf("recording status is %s but should be %s", sess.RecordingStatus, api.RecordingStatusWaiting)
		return 250, err
	}
	if err = rt.isCancelled(); err != nil {
		return 0, err
	}

	glog.Infof("Streaming done, waiting for recording URL to appear. streamId=%s playbackId=%s", stream.ID, stream.PlaybackID)
	if rt.useForceURL {
		time.Sleep(5 * time.Second)
	} else {
		time.Sleep(rt.recordingWaitTime)
	}
	if err = rt.isCancelled(); err != nil {
		return 0, err
	}

	sessions, err = rt.lapi.GetSessionsNew(stream.ID, rt.useForceURL)
	if err != nil {
		err := fmt.Errorf("error getting sessions for stream id=%s err=%v", stream.ID, err)
		return 252, err
	}
	glog.V(model.DEBUG).Infof("Sessions: %+v streamId=%s playbackId=%s", sessions, stream.ID, stream.PlaybackID)
	if err = rt.isCancelled(); err != nil {
		return 0, err
	}

	lapiNoAPIKey := api.NewAPIClient(api.ClientOptions{
		Server:      rt.lapi.GetServer(),
		AccessToken: "", // test playback info call without API key
		Timeout:     8 * time.Second,
	})
	if code, err := checkPlaybackInfo(stream.PlaybackID, rt.lapi, lapiNoAPIKey); err != nil {
		return code, err
	}
	for _, sess := range sessions {
		statusShould := api.RecordingStatusReady
		if rt.useForceURL {
			statusShould = api.RecordingStatusWaiting
		}
		if sess.RecordingStatus != statusShould {
			err := fmt.Errorf("recording status is %s but should be %s", sess.RecordingStatus, statusShould)
			return 240, err
		}
		if sess.RecordingURL == "" {
			err := fmt.Errorf("recording URL should appear by now")
			return 249, err
		}
		glog.Infof("recordingURL=%s downloading now. streamId=%s playbackId=%s", sess.RecordingURL, stream.ID, stream.PlaybackID)

		if err = rt.isCancelled(); err != nil {
			return 0, err
		}
		if rt.mp4 {
			es, err := rt.checkDownMp4(stream, sess.Mp4Url, testDuration)
			if err != nil {
				return es, err
			}
		}

		es, err := rt.checkDown(stream, sess.RecordingURL, testDuration)
		if err != nil {
			return es, err
		}

		// currently the assetID is the same as the sessionID so we could just query on that but just in case that
		// ever changes, we can use the ListAssets call to find the asset
		assets, _, err := rt.lapi.ListAssets(api.ListOptions{
			Limit: 1,
			Filters: map[string]interface{}{
				"sourceSessionId": sess.ID,
			},
		})
		if err != nil {
			return 248, err
		}

		if len(assets) != 1 {
			return 247, fmt.Errorf("unexpected number of assets. expected: 1 actual: %d", len(assets))
		}
		if !assets[0].SourcePlaybackReady {
			return 246, fmt.Errorf("source playback was not ready")
		}

		if code, err := checkPlaybackInfo(assets[0].PlaybackID, rt.lapi, lapiNoAPIKey); err != nil {
			return code, err
		}
	}
	glog.Infof("Done Record Test. streamId=%s playbackId=%s", stream.ID, stream.PlaybackID)

	rt.lapi.DeleteStream(stream.ID)
	return 0, nil
}

func checkPlaybackInfo(playbackID string, withKey, withoutKey *api.Client) (int, error) {
	playbackInfo, err := withKey.GetPlaybackInfo(playbackID)
	if err != nil {
		return 245, fmt.Errorf("playback info call failed for %s: %w", playbackID, err)
	}
	if len(playbackInfo.Meta.Source) <= 0 {
		return 244, fmt.Errorf("expected at least one source returned for %s", playbackID)
	}
	playbackInfo, err = withoutKey.GetPlaybackInfo(playbackID)
	if err != nil {
		return 245, fmt.Errorf("playback info call without API key failed for %s: %w", playbackID, err)
	}
	if len(playbackInfo.Meta.Source) <= 0 {
		return 244, fmt.Errorf("expected at least one source returned without API key for %s", playbackID)
	}
	return 0, nil
}

func (rt *recordTester) getIngestInfo() (*api.Ingest, error) {
	if rt.ingest != nil {
		return rt.ingest, nil
	}
	var ingests []api.Ingest
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

func (rt *recordTester) doOneHTTPStream(fileName, streamName, broadcasterURL string, testDuration time.Duration, stream *api.Stream) error {
	var session *api.Stream
	var err error
	apiTry := 0
	for {
		session, err = rt.lapi.CreateStream(api.CreateStreamReq{Name: streamName, Record: true, RecordObjectStoreId: rt.recordObjectStoreId, ParentID: stream.ID})
		if err != nil {
			if testers.Timedout(err) && apiTry < 3 {
				apiTry++
				continue
			}
			glog.Errorf("Error creating stream session using Livepeer API: %v", err)
			return err
		}
		break
	}
	hs := testers.NewHTTPStreamer(rt.ctx, false, "not used")
	httpIngestBaseURL := fmt.Sprintf("%s/live/%s", broadcasterURL, session.ID)
	glog.Infof("httpIngestBaseURL=%s", httpIngestBaseURL)
	hs.StartUpload(fileName, httpIngestBaseURL, stream.ID, -1, -1, testDuration, 0)

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

func (rt *recordTester) checkDownMp4(stream *api.Stream, url string, streamDuration time.Duration) (int, error) {
	es := 0
	started := time.Now()
	glog.V(model.VERBOSE).Infof("Downloading mp4 url=%s streamId=%s playbackId=%s", url, stream.ID, stream.PlaybackID)

	resp, err := http.Get(url)
	if err != nil {
		glog.Warningf("Error downloading mp4 url=%s streamId=%s playbackId=%s", url, stream.ID, stream.PlaybackID)
		return 3, err
	}
	if resp.StatusCode != http.StatusOK {
		glog.Warningf("HTTP error downloading mp4 url=%s status=%s streamId=%s playbackId=%s", url, resp.Status, stream.ID, stream.PlaybackID)
		return 3, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.Warningf("HTTP error downloading mp4 url=%s err=%v streamId=%s playbackId=%s", url, err, stream.ID, stream.PlaybackID)
		return 3, err
	}
	glog.Infof("Downloaded bytes=%b url=%s took=%s streamId=%s playbackId=%s", len(body), url, time.Since(started), stream.ID, stream.PlaybackID)
	bodyR := bytes.NewReader(body)
	dem := mp4.NewDemuxer(bodyR)
	streams, err := dem.Streams()
	if err != nil {
		glog.Warningf("Error parsing mp4 url=%s err=%v streamId=%s playbackId=%s", url, err, stream.ID, stream.PlaybackID)
		return 203, err
	}
	if len(streams) != 2 {
		ers := fmt.Errorf("expected only 2 streams (video and audio), got=%d, streams=%v", len(streams), streams)
		glog.Error(ers)
		return 203, err
	}
	glog.Infof("Got %d streams in mp4 file url=%s streamId=%s playbackId=%s", len(streams), url, stream.ID, stream.PlaybackID)
	dur, err := calcMP4FileDuration(body)
	if err != nil {
		glog.Warningf("Error parsing mp4 url=%s err=%v streamId=%s playbackId=%s", url, err, stream.ID, stream.PlaybackID)
		return 203, err
	}
	durDiff := streamDuration - dur
	if durDiff < 0 {
		durDiff = -durDiff
	}
	if durDiff > 2*time.Second {
		ers := fmt.Errorf("duration of mp4 differ by %s (got %s, should %s)", durDiff, dur, streamDuration)
		glog.Error(ers)
		return 300, ers
	}
	return es, nil
}

func (rt *recordTester) checkDown(stream *api.Stream, url string, streamDuration time.Duration) (int, error) {
	es := 0
	started := time.Now()
	downloader := testers.NewM3utester2(rt.ctx, url, false, false, false, false, 5*time.Second, nil, false)
	<-downloader.Done()
	glog.Infof(`Pulling stopped after %s. streamId=%s playbackId=%s`, time.Since(started), stream.ID, stream.PlaybackID)
	if err := rt.isCancelled(); err != nil {
		return 0, err
	}
	vs := downloader.VODStats()
	rt.vodStats = vs
	if len(vs.SegmentsNum) != len(api.StandardProfiles)+1 {
		glog.Warningf("Number of renditions doesn't match! Has %d should %d. streamId=%s playbackId=%s", len(vs.SegmentsNum), len(api.StandardProfiles)+1, stream.ID, stream.PlaybackID)
		es = 35
	}
	glog.V(model.DEBUG).Infof("Stats: %s streamId=%s playbackId=%s", vs.String(), stream.ID, stream.PlaybackID)
	glog.V(model.DEBUG).Infof("Stats raw: %+v streamId=%s playbackId=%s", vs, stream.ID, stream.PlaybackID)
	if ok, ers := vs.IsOk(streamDuration, false); !ok {
		glog.Warningf("NOT OK! (%s) streamId=%s playbackId=%s", ers, stream.ID, stream.PlaybackID)
		es = 36
		return es, errors.New(ers)
	} else {
		glog.Infof("All ok! streamId=%s playbackId=%s", stream.ID, stream.PlaybackID)
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

func (rt *recordTester) Stream() *api.Stream {
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
