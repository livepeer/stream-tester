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
	"strings"
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

const mp4DurationDiffTolerance = 6 * time.Second

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
		RecordingSpec       *api.RecordingSpec
		SkipSourcePlayback  bool
		UseForceURL         bool
		RecordingWaitTime   time.Duration
		UseHTTP             bool
		TestMP4             bool
		TestStreamHealth    bool
	}

	recordTester struct {
		RecordTesterOptions
		serfOpts SerfOptions

		ctx    context.Context
		cancel context.CancelFunc

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
		RecordTesterOptions: opts,
		ctx:                 ctx,
		cancel:              cancel,
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
		broadcasters, err = rt.API.Broadcasters()
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

	if rt.UseHTTP && len(broadcasters) == 0 {
		return 254, errors.New("empty list of broadcasters")
	} else if (!rt.UseHTTP && ingest.Ingest == "") || ingest.Playback == "" {
		return 254, errors.New("empty ingest URLs")
	}

	hostName, _ := os.Hostname()
	streamName := fmt.Sprintf("%s_%s", hostName, time.Now().Format("2006-01-02T15:04:05Z07:00"))
	var stream *api.Stream
	for {
		stream, err = rt.API.CreateStream(api.CreateStreamReq{
			Name:                streamName,
			Record:              true,
			RecordingSpec:       rt.RecordingSpec,
			RecordObjectStoreId: rt.RecordObjectStoreId,
		})
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
	if rt.TestStreamHealth {
		testerFuncs = append(testerFuncs, func(ctx context.Context, mediaURL string, waitForTarget time.Duration, opts testers.Streamer2Options) testers.Finite {
			return testers.NewStreamHealth(ctx, stream.ID, rt.Analyzers, 2*time.Minute)
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
	if rt.UseHTTP {
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

	lapiNoAPIKey := api.NewAPIClient(api.ClientOptions{
		Server:      rt.API.GetServer(),
		AccessToken: "", // test playback info call without API key
		Timeout:     8 * time.Second,
	})
	if code, err := checkPlaybackInfo(stream.PlaybackID, rt.API, lapiNoAPIKey); err != nil {
		return code, err
	}

	glog.Infof("Waiting 10 seconds. streamId=%s playbackId=%s", stream.ID, stream.PlaybackID)
	time.Sleep(10 * time.Second)
	// now get sessions
	sessions, err := rt.API.GetSessionsNew(stream.ID, false)
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

	deadline := time.Now().Add(rt.RecordingWaitTime)
	if rt.UseForceURL {
		deadline = time.Now().Add(5 * time.Second)
	}

	// For checking if sourcePlayback was available we see if at least 1 session
	// recording (asset) got a playbackUrl before the processing was done.
	var sourcePlayback bool
	for errCode, errs := -1, []error{}; errCode != 0; {
		if time.Now().After(deadline) {
			errsStrs := make([]string, len(errs))
			for i, err := range errs {
				errsStrs[i] = err.Error()
			}
			err := fmt.Errorf("timeout waiting for recording URL to appear: %s", strings.Join(errsStrs, "; "))
			return errCode, err
		} else if err = rt.isCancelled(); err != nil {
			return 0, err
		}
		time.Sleep(5 * time.Second)

		errCode, errs = 0, nil
		for _, sess := range sessions {
			// currently the assetID is the same as the sessionID so we could just query on that but just in case that
			// ever changes, we can use the ListAssets call to find the asset
			assets, _, err := rt.API.ListAssets(api.ListOptions{
				Limit: 1,
				Filters: map[string]interface{}{
					"sourceSessionId": sess.ID,
				},
			})
			if err != nil {
				errCode, errs = 248, append(errs, err)
				continue
			}

			if len(assets) != 1 {
				err := fmt.Errorf("unexpected number of assets. expected: 1 actual: %d", len(assets))
				errCode, errs = 247, append(errs, err)
				continue
			}
			asset := assets[0]

			if code, err := checkPlaybackInfo(asset.PlaybackID, rt.API, lapiNoAPIKey); err != nil {
				errCode, errs = code, append(errs, err)
			} else {
				// if we get playback before the processing is done it means source playback was provided
				if asset.Status.Phase != "ready" {
					sourcePlayback = true
				}
			}

			if asset.Status.Phase != "ready" {
				err := fmt.Errorf("asset status is %s but should be ready", asset.Status.Phase)
				errCode, errs = 246, append(errs, err)
			}
		}
	}
	if !sourcePlayback && !rt.SkipSourcePlayback {
		return 246, errors.New("source playback was not provided")
	}

	// check actual recordings playback
	sessions, err = rt.API.GetSessionsNew(stream.ID, false)
	if err != nil {
		glog.Errorf("Error getting sessions err=%v streamId=%s playbackId=%s", err, stream.ID, stream.PlaybackID)
		return 252, err
	}
	glog.V(model.DEBUG).Infof("Sessions: %+v streamId=%s playbackId=%s", sessions, stream.ID, stream.PlaybackID)

	if len(sessions) != expectedSessions {
		err := fmt.Errorf("invalid session count, expected %d but got %d",
			expectedSessions, len(sessions))
		glog.Error(err)
		return 251, err
	}

	for _, sess := range sessions {
		statusShould := api.RecordingStatusReady
		if rt.UseForceURL {
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
		if rt.TestMP4 {
			es, err := rt.checkRecordingMp4(stream, sess.Mp4Url, testDuration)
			if err != nil {
				return es, err
			}
		}

		if err = rt.isCancelled(); err != nil {
			return 0, err
		}
		es, err := rt.checkRecordingHls(stream, sess.RecordingURL, testDuration)
		if err != nil {
			return es, err
		}
	}

	glog.Infof("Done Record Test. streamId=%s playbackId=%s", stream.ID, stream.PlaybackID)

	rt.API.DeleteStream(stream.ID)
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
	if rt.Ingest != nil {
		return rt.Ingest, nil
	}
	var ingests []api.Ingest
	apiTry := 0
	for {
		var err error
		ingests, err = rt.API.Ingest(false)
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
		session, err = rt.API.CreateStream(api.CreateStreamReq{
			Name:                streamName,
			Record:              true,
			RecordingSpec:       rt.RecordingSpec,
			RecordObjectStoreId: rt.RecordObjectStoreId,
			ParentID:            stream.ID,
		})
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

func (rt *recordTester) checkRecordingMp4(stream *api.Stream, url string, streamDuration time.Duration) (int, error) {
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
	if durDiff > mp4DurationDiffTolerance {
		ers := fmt.Errorf("duration of mp4 differ by %s (got %s, should %s)", durDiff, dur, streamDuration)
		glog.Error(ers)
		return 300, ers
	}
	return es, nil
}

func (rt *recordTester) checkRecordingHls(stream *api.Stream, url string, streamDuration time.Duration) (int, error) {
	started := time.Now()
	downloader := testers.NewM3utester2(rt.ctx, url, false, false, false, false, 5*time.Second, nil, false)
	<-downloader.Done()
	glog.Infof(`Pulling stopped after %s. streamId=%s playbackId=%s`, time.Since(started), stream.ID, stream.PlaybackID)
	if err := rt.isCancelled(); err != nil {
		return 0, err
	}
	vs := downloader.VODStats()
	rt.vodStats = vs

	numProfiles := len(vs.SegmentsNum)
	if rt.RecordingSpec != nil && rt.RecordingSpec.Profiles != nil {
		expectedProfiles := len(*rt.RecordingSpec.Profiles) + 1
		if numProfiles != expectedProfiles {
			glog.Warningf("Number of renditions doesn't match! Has %d should %d. streamId=%s playbackId=%s", numProfiles, expectedProfiles, stream.ID, stream.PlaybackID)
			return 35, fmt.Errorf("number of renditions doesn't match (expected: %d actual: %d)", expectedProfiles, numProfiles)
		}
	} else {
		// if there's no explicit recording spec we can only expect there's at least 2 profiles (source and transcoded)
		expectedProfiles := 2
		if numProfiles < expectedProfiles {
			glog.Warningf("Number of renditions too low! Has %d should have at least %d. streamId=%s playbackId=%s", numProfiles, expectedProfiles, stream.ID, stream.PlaybackID)
			return 35, fmt.Errorf("number of renditions too low (expected at least: %d actual: %d)", expectedProfiles, numProfiles)
		}
	}

	glog.V(model.DEBUG).Infof("Stats: %s streamId=%s playbackId=%s", vs.String(), stream.ID, stream.PlaybackID)
	glog.V(model.DEBUG).Infof("Stats raw: %+v streamId=%s playbackId=%s", vs, stream.ID, stream.PlaybackID)
	if ok, ers := vs.IsOk(streamDuration, false); !ok {
		glog.Warningf("NOT OK! (%s) streamId=%s playbackId=%s", ers, stream.ID, stream.PlaybackID)
		return 36, errors.New(ers)
	} else {
		glog.Infof("All ok! streamId=%s playbackId=%s", stream.ID, stream.PlaybackID)
	}
	return 0, nil
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
		rt.API.DeleteStream(rt.streamID)
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
