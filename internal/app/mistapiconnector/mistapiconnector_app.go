package mistapiconnector

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/go-api-client"
	"github.com/livepeer/livepeer-data/pkg/data"
	"github.com/livepeer/livepeer-data/pkg/event"
	"github.com/livepeer/stream-tester/apis/mist"
	"github.com/livepeer/stream-tester/internal/metrics"
	"github.com/livepeer/stream-tester/internal/utils"
	"github.com/livepeer/stream-tester/model"
	amqp "github.com/rabbitmq/amqp091-go"
)

const streamPlaybackPrefix = "playback_"
const audioAlways = "always"
const audioRecord = "record"
const audioEnabledStreamSuffix = "rec"
const waitForPushError = 7 * time.Second
const keepStreamAfterEnd = 15 * time.Second
const statsCollectionPeriod = 10 * time.Second

const ownExchangeName = "lp_mist_api_connector"
const webhooksExchangeName = "webhook_default_exchange"
const eventMultistreamConnected = "multistream.connected"
const eventMultistreamError = "multistream.error"
const eventMultistreamDisconnected = "multistream.disconnected"

type (
	// IMac creates new Mist API Connector application
	IMac interface {
		SetupTriggers(ownURI string) error
		StartServer(bindAddr string) error
		SrvShutCh() chan error
	}

	pushStatus struct {
		target           *api.MultistreamTarget
		profile          string
		pushStartEmitted bool
		pushStopped      bool
		metrics          *data.MultistreamMetrics
	}

	streamInfo struct {
		id        string
		stream    *api.Stream
		startedAt time.Time

		mu                 sync.Mutex
		done               chan struct{}
		stopped            bool
		multistreamStarted bool
		pushStatus         map[string]*pushStatus
	}

	trackListDesc struct {
		Bps      int64  `json:"bps,omitempty"`
		Channels int    `json:"channels,omitempty"`
		Codec    string `json:"codec,omitempty"`
		Firstms  int64  `json:"firstms,omitempty"`
		Idx      int    `json:"idx,omitempty"`
		Init     string `json:"init,omitempty"`
		Jitter   int    `json:"jitter,omitempty"`
		Lastms   int64  `json:"lastms,omitempty"`
		Maxbps   int64  `json:"maxbps,omitempty"`
		Rate     int    `json:"rate,omitempty"`
		Size     int    `json:"size,omitempty"`
		Trackid  int    `json:"trackid,omitempty"`
		Type     string `json:"type,omitempty"`
		Bframes  int    `json:"bframes,omitempty"`
		Fpks     int64  `json:"fpks,omitempty"`
		Width    int    `json:"width,omitempty"`
		Height   int    `json:"height,omitempty"`
	}

	// MacOptions configuration object
	MacOptions struct {
		NodeID, MistHost string
		MistAPI          *mist.API
		LivepeerAPI      *api.Client
		BalancerHost     string
		CheckBandwidth   bool
		RoutePrefix, PlaybackDomain, MistURL,
		SendAudio, BaseStreamName string
		AMQPUrl, OwnRegion        string
		MistStreamSource          string
		MistHardcodedBroadcasters string
		NoMistScrapeMetrics       bool
	}

	trackList map[string]*trackListDesc

	mac struct {
		ctx                       context.Context
		cancel                    context.CancelFunc
		mapi                      *mist.API
		lapi                      *api.Client
		balancerHost              string
		srv                       *http.Server
		srvShutCh                 chan error
		mu                        sync.RWMutex
		createStreamLock          sync.Mutex
		mistHot                   string
		checkBandwidth            bool
		routePrefix               string
		mistURL                   string
		playbackDomain            string
		sendAudio                 string
		baseStreamName            string
		streamInfo                map[string]*streamInfo // public key to info
		producer                  *event.AMQPProducer
		nodeID                    string
		ownRegion                 string
		mistStreamSource          string
		mistHardcodedBroadcasters string
	}
)

// NewMac ...
func NewMac(opts MacOptions) (IMac, error) {
	if opts.BalancerHost != "" && !strings.Contains(opts.BalancerHost, ":") {
		opts.BalancerHost = opts.BalancerHost + ":8042" // must set default port for Mist's Load Balancer
	}
	ctx, cancel := context.WithCancel(context.Background())
	var producer *event.AMQPProducer
	if opts.AMQPUrl != "" {
		pu, err := url.Parse(opts.AMQPUrl)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("error parsing AMQP url err=%w", err)
		}

		glog.Infof("Creating AMQP producer with url=%s", pu.Redacted())
		setup := func(c *amqp.Channel) error {
			if err := c.ExchangeDeclarePassive(webhooksExchangeName, "topic", true, false, false, false, nil); err != nil {
				glog.Warningf("mist-api-connector: Webhooks exchange does not exist. exchange=%s err=%v", webhooksExchangeName, err)
			}
			return c.ExchangeDeclare(ownExchangeName, "topic", true, false, false, false, nil)
		}
		producer, err = event.NewAMQPProducer(opts.AMQPUrl, event.NewAMQPConnectFunc(setup))
		if err != nil {
			cancel()
			return nil, err
		}
	} else {
		glog.Infof("AMQP url is empty!")
	}
	mc := &mac{
		nodeID:                    opts.NodeID,
		mistHot:                   opts.MistHost,
		mapi:                      opts.MistAPI,
		lapi:                      opts.LivepeerAPI,
		checkBandwidth:            opts.CheckBandwidth,
		balancerHost:              opts.BalancerHost,
		streamInfo:                make(map[string]*streamInfo),
		routePrefix:               opts.RoutePrefix,
		mistURL:                   opts.MistURL,
		playbackDomain:            opts.PlaybackDomain,
		sendAudio:                 opts.SendAudio,
		baseStreamName:            opts.BaseStreamName,
		srvShutCh:                 make(chan error),
		ctx:                       ctx,
		cancel:                    cancel,
		producer:                  producer,
		ownRegion:                 opts.OwnRegion,
		mistStreamSource:          opts.MistStreamSource,
		mistHardcodedBroadcasters: opts.MistHardcodedBroadcasters,
	}
	if producer != nil && !opts.NoMistScrapeMetrics {
		startMetricsCollector(ctx, statsCollectionPeriod, opts.NodeID, opts.OwnRegion, opts.MistAPI, producer, ownExchangeName, mc)
	}
	return mc, nil
}

// LivepeerProfiles2MistProfiles converts Livepeer's API profiles to Mist's ones
func LivepeerProfiles2MistProfiles(lps []api.Profile) []mist.Profile {
	var res []mist.Profile
	for _, p := range lps {
		mp := mist.Profile{
			Name:      p.Name,
			HumanName: p.Name,
			Width:     p.Width,
			Height:    p.Height,
			Fps:       p.Fps,
			Bitrate:   p.Bitrate,
		}
		res = append(res, mp)
	}
	return res
}

func (mc *mac) triggerLiveBandwidth(w http.ResponseWriter, r *http.Request) bool {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("yes"))
	return false
}

func (mc *mac) triggerConnClose(w http.ResponseWriter, r *http.Request, lines []string, rawRequest string) bool {
	if len(lines) < 3 {
		glog.Errorf("Expected 3 lines, got %d, request \n%s", len(lines), rawRequest)
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("false"))
		return false
	}
	protocol := lines[2]
	// RTMP PUSH_END sends CONN_CLOSE too, but it has empty last line
	// TSSRT PUSH_END also sends CONN_CLOSE, but it has an empty _second_ line.
	// TODO FIXME this was a quick hack to unbreak TSSRT and could break at any time.
	// The right way to solve this is to "Use the STREAM_BUFFER trigger and look for
	// the EMPTY state field in the payload."
	if (protocol == "RTMP" && len(lines[3]) > 0) || (protocol == "TSSRT" && len(lines[1]) > 0) {
		playbackID := strings.TrimPrefix(lines[0], streamPlaybackPrefix)
		if mc.baseStreamName != "" && strings.Contains(playbackID, "+") {
			playbackID = strings.Split(playbackID, "+")[1]
		}
		if info, ok := mc.getStreamInfoLogged(playbackID); ok {
			glog.Infof("Setting stream's protocol=%s manifestID=%s playbackID=%s active status to false", protocol, info.id, playbackID)
			_, err := mc.lapi.SetActive(info.id, false, info.startedAt)
			if err != nil {
				glog.Error(err)
			}
			mc.emitStreamStateEvent(info.stream, data.StreamState{Active: false})
			info.mu.Lock()
			info.stopped = true
			info.mu.Unlock()
			mc.removeInfoDelayed(playbackID, info.done)
			metrics.StopStream(true)
		}
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("yes"))
	return true
}

func (mc *mac) triggerDefaultStream(w http.ResponseWriter, r *http.Request, lines []string, trigger string) bool {
	if mc.balancerHost == "" {
		glog.V(model.VERBOSE).Infof("Request %s: (%d lines) responded with forbidden", trigger, len(lines))
		w.WriteHeader(http.StatusForbidden)
		w.Write([]byte("false"))
		return false
	}
	if len(lines) == 5 {
		protocol := lines[3] // HLS
		uri := lines[4]      // /hls/h5rfoaiqoafbsq44/index.m3u8?stream=h5rfoaiqoafbsq44
		if protocol == "HLS" {
			urip := strings.Split(uri, "/")
			if len(urip) > 2 {
				glog.Infof("proto: %s uri parts: %+v", protocol, urip)
				// urip[2] = streamPlaybackPrefix + urip[2]
				playbackID := urip[2]
				streamNameInMist := streamPlaybackPrefix + playbackID
				// hold the creation lock until exit so that another trigger to
				// RTMP_REWRITE can't create a Mist stream at the same time
				mc.createStreamLock.Lock()
				defer mc.createStreamLock.Unlock()

				if info, ok := mc.getStreamInfoLogged(playbackID); ok {
					info.mu.Lock()
					streamStopped := info.stopped
					info.mu.Unlock()
					if streamStopped {
						mc.removeInfo(playbackID)
					} else {
						// that means that RTMP stream is currently gets streamed into our Mist node
						// and so no changes needed to the Mist configuration
						glog.Infof("Already in the playing map, returning %s", streamNameInMist)
						w.WriteHeader(http.StatusOK)
						w.Write([]byte(streamNameInMist))
						return true
					}
				}

				// check if such stream already exists in Mist's config
				streams, activeStreams, err := mc.mapi.Streams()
				if err != nil {
					glog.Warningf("Error getting streams list from Mist: %v", err)
					w.WriteHeader(http.StatusOK)
					w.Write([]byte(streamNameInMist))
					return true
				}
				if utils.StringsSliceContains(activeStreams, streamNameInMist) {
					glog.Infof("Stream is in active map, returning %s", streamNameInMist)
					w.WriteHeader(http.StatusOK)
					w.Write([]byte(streamNameInMist))
					return true
				}
				if mstream, has := streams[streamNameInMist]; has {
					if len(mstream.Processes) == 0 {
						// Stream exists and has transcoding turned off
						glog.Infof("Requested stream '%s' already exists in Mist config, just returning it's name", streamNameInMist)
						w.WriteHeader(http.StatusOK)
						w.Write([]byte(streamNameInMist))
						return true
					}
				}
				// Looks like there is no RTMP stream on our Mist server, so probably it is on other
				// (load balanced) server. So we need to create Mist's stream configuration without
				// transcoding
				stream, err := mc.lapi.GetStreamByPlaybackID(playbackID)
				if err != nil || stream == nil {
					glog.Errorf("Error getting stream info from Livepeer API err=%v", err)
					w.WriteHeader(http.StatusNotFound)
					w.Write([]byte("false"))
					return true
				}
				glog.V(model.DEBUG).Infof("For stream %s got info %+v", playbackID, stream)
				if stream.Deleted {
					glog.Infof("Stream %s was deleted, so deleting Mist's stream configuration", playbackID)
					go mc.mapi.DeleteStreams(streamNameInMist)
					w.WriteHeader(http.StatusNotFound)
					w.Write([]byte("false"))
					return true
				}
				err = mc.createMistStream(streamNameInMist, stream, true)
				if err != nil {
					glog.Errorf("Error creating stream on the Mist server: %v", err)
				}
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(streamNameInMist))
				return true
			}
		}
	}
	// We should get this in two cases:
	// 1. When in PUSH_REWRITE we got request for unknown stream and thus
	//    haven't created new stream in Mist
	// 2. When someone pulls HLS for stream that exists but is not active (no
	//    RTMP stream coming in).
	w.WriteHeader(http.StatusForbidden)
	w.Write([]byte("false"))
	return true
}

func (mc *mac) triggerPushRewrite(w http.ResponseWriter, r *http.Request, lines []string, rawRequest string) bool {
	if len(lines) != 3 {
		glog.Errorf("Expected 3 lines, got %d, request \n%s", len(lines), rawRequest)
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("false"))
		return false
	}
	// glog.V(model.INSANE).Infof("Parsed request (%d):\n%+v", len(lines), lines)
	pu, err := url.Parse(lines[0])
	streamKey := lines[2]
	var responseName string
	if err != nil {
		glog.Errorf("Error parsing url=%s err=%v", lines[0], err)
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("false"))
		return false
	}
	if pu.Scheme == "rtmp" {
		pp := strings.Split(pu.Path, "/")
		if len(pp) != 3 {
			glog.Errorf("Push rewrite URL wrongly formatted - should be in format rtmp://mist.host/live/streamKey")
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("false"))
			return false
		}
	}
	glog.V(model.VVERBOSE).Infof("Requested stream key is '%s'", streamKey)
	// ask API
	stream, err := mc.lapi.GetStreamByKey(streamKey)
	if err != nil || stream == nil {
		glog.Errorf("Error getting stream info from Livepeer API streamKey=%s err=%v", streamKey, err)
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("false"))
		return false
	}
	glog.V(model.VERBOSE).Infof("For stream %s got info %+v", streamKey, stream)

	if stream.Deleted {
		glog.Infof("Stream %s was deleted, so deleting Mist's stream configuration", streamKey)
		if mc.baseStreamName == "" {
			streamKey = stream.PlaybackID
			// streamKey = strings.ReplaceAll(streamKey, "-", "")
			if mc.balancerHost != "" {
				streamKey = streamPlaybackPrefix + streamKey
			}
			mc.mapi.DeleteStreams(streamKey)
		}
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte("false"))
		return false
	}

	if stream.PlaybackID != "" {
		mc.mu.Lock()
		if info, ok := mc.streamInfo[stream.PlaybackID]; ok {
			info.mu.Lock()
			glog.Infof("Stream playbackID=%s stopped=%v already in map, removing its info", stream.PlaybackID, info.stopped)
			info.mu.Unlock()
			mc.removeInfoLocked(stream.PlaybackID)
		}
		info := &streamInfo{
			id:         stream.ID,
			stream:     stream,
			done:       make(chan struct{}),
			pushStatus: make(map[string]*pushStatus),
			startedAt:  time.Now(),
		}
		mc.streamInfo[stream.PlaybackID] = info
		mc.mu.Unlock()
		streamKey = stream.PlaybackID
		// streamKey = strings.ReplaceAll(streamKey, "-", "")
		if mc.balancerHost != "" {
			streamKey = streamPlaybackPrefix + streamKey
		}
		if mc.baseStreamName == "" {
			responseName = streamKey
		} else {
			responseName = mc.wildcardPlaybackID(stream)
		}
		ok, err := mc.lapi.SetActive(stream.ID, true, info.startedAt)
		if err != nil {
			glog.Error(err)
		} else if !ok {
			glog.Infof("Stream id=%s streamKey=%s playbackId=%s forbidden by webhook, rejecting", stream.ID, stream.StreamKey, stream.PlaybackID)
			mc.removeInfo(stream.PlaybackID)
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte("false"))
			return true
		}
	} else {
		glog.Errorf("Shouldn't happen streamID=%s", stream.ID)
		// streamKey = strings.ReplaceAll(streamKey, "-", "")
	}
	if mc.baseStreamName == "" {
		err = mc.createMistStream(streamKey, stream, false)
		if err != nil {
			glog.Errorf("Error creating stream on the Mist server: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("false"))
			return true
		}
	}
	go mc.emitStreamStateEvent(stream, data.StreamState{Active: true})
	w.Write([]byte(responseName))
	metrics.StartStream()
	glog.Infof("Responded with '%s'", responseName)
	return true
}

func (mc *mac) triggerLiveTrackList(w http.ResponseWriter, r *http.Request, lines []string, rawRequest string) bool {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("yes"))
	if len(lines) < 2 {
		glog.Errorf("Expected 2 lines, got %d, request \n%s", len(lines), rawRequest)
		return false
	}
	go func() {
		var tl trackList
		err := json.Unmarshal([]byte(lines[1]), &tl)
		if err != nil {
			glog.Errorf("Error unmurshalling json track list: %v", err)
			return
		}
		videoTracksNum := tl.CountVideoTracks()
		playbackID := mistStreamName2playbackID(lines[0])
		glog.Infof("for video %s got %d video tracks", playbackID, videoTracksNum)
		if info, ok := mc.getStreamInfoLogged(playbackID); ok {
			info.mu.Lock()
			alreadyStarted := info.multistreamStarted
			info.multistreamStarted = true
			info.mu.Unlock()
			if !alreadyStarted && len(info.stream.Multistream.Targets) > 0 && videoTracksNum > 1 {
				mc.startMultistream(lines[0], playbackID, info)
			}
		}
	}()
	return true
}

func (mc *mac) triggerPushOutStart(w http.ResponseWriter, r *http.Request, lines []string, rawRequest string) bool {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("yes"))
	if len(lines) < 2 {
		glog.Errorf("Expected 2 lines, got %d, request \n%s", len(lines), rawRequest)
		return false
	}
	go func() {
		playbackID := mistStreamName2playbackID(lines[0])
		if info, ok := mc.getStreamInfoLogged(playbackID); ok {
			info.mu.Lock()
			defer info.mu.Unlock()
			if pushInfo, ok := info.pushStatus[lines[1]]; ok {
				go mc.waitPush(info, pushInfo)
			} else {
				glog.Errorf("For stream playbackID=%s got unknown RTMP push %s", playbackID, lines[1])
			}
		}

	}()
	return true
}

// waits for RTMP push error
func (mc *mac) waitPush(info *streamInfo, pushInfo *pushStatus) {
	select {
	case <-info.done:
		return
	case <-time.After(waitForPushError):
		info.mu.Lock()
		defer info.mu.Unlock()
		if info.stopped {
			return
		}
		if !pushInfo.pushStopped {
			// there was no error starting RTMP push, so no we can send 'multistream.connected' webhook event
			pushInfo.pushStartEmitted = true
			mc.emitWebhookEvent(info.stream, pushInfo, eventMultistreamConnected)
		}
	}
}

func (mc *mac) emitStreamStateEvent(stream *api.Stream, state data.StreamState) {
	streamID := stream.ParentID
	if streamID == "" {
		streamID = stream.ID
	}
	stateEvt := data.NewStreamStateEvent(mc.nodeID, mc.ownRegion, stream.UserID, streamID, state)
	mc.emitAmqpEvent(ownExchangeName, "stream.state."+streamID, stateEvt)
}

func (mc *mac) emitWebhookEvent(stream *api.Stream, pushInfo *pushStatus, eventKey string) {
	streamID, sessionID := stream.ParentID, stream.ID
	if streamID == "" {
		streamID = sessionID
	}
	payload := data.MultistreamWebhookPayload{
		Target: pushToMultistreamTargetInfo(pushInfo),
	}
	hookEvt, err := data.NewWebhookEvent(streamID, eventKey, stream.UserID, sessionID, payload)
	if err != nil {
		glog.Errorf("Error creating webhook event err=%v", err)
		return
	}
	mc.emitAmqpEvent(webhooksExchangeName, "events."+eventKey, hookEvt)
}

func (mc *mac) emitAmqpEvent(exchange, key string, evt data.Event) {
	if mc.producer == nil {
		return
	}
	glog.Infof("Publishing amqp message to exchange=%s key=%s msg=%+v", exchange, key, evt)

	ctx, cancel := context.WithTimeout(mc.ctx, 3*time.Second)
	defer cancel()
	err := mc.producer.Publish(ctx, event.AMQPMessage{
		Exchange:   exchange,
		Key:        key,
		Body:       evt,
		Persistent: true,
	})
	if err != nil {
		glog.Errorf("Error publishing amqp message to exchange=%s key=%s, err=%v", exchange, key, err)
	}
}

func (mc *mac) triggerPushEnd(w http.ResponseWriter, r *http.Request, lines []string, rawRequest string) bool {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("yes"))
	if len(lines) < 3 {
		glog.Errorf("Expected 6 lines, got %d, request \n%s", len(lines), rawRequest)
		return false
	}
	go func() {
		playbackID := mistStreamName2playbackID(lines[1])
		// glog.Infof("for video %s got %d video tracks", playbackID, videoTracksNum)
		if info, ok := mc.getStreamInfoLogged(playbackID); ok {
			info.mu.Lock()
			defer info.mu.Unlock()
			if pushInfo, ok := info.pushStatus[lines[2]]; ok {
				if pushInfo.pushStartEmitted {
					// emit normal push.end
					mc.emitWebhookEvent(info.stream, pushInfo, eventMultistreamDisconnected)
				} else {
					pushInfo.pushStopped = true
					//  emit push error
					mc.emitWebhookEvent(info.stream, pushInfo, eventMultistreamError)
				}
			} else {
				glog.Errorf("For stream playbackID=%s got unknown RTMP push %s", playbackID, lines[1])
			}
		}
	}()
	return true
}

func (mc *mac) handleDefaultStreamTrigger(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		w.Write([]byte("false"))
		return
	}
	b, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("false"))
		return
	}
	bs := string(b)
	lines := strings.Split(bs, "\n")
	trigger := r.Header.Get("X-Trigger")
	if trigger == "" {
		glog.Errorf("Trigger not defined in request %s", bs)
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("false"))
		return
	}
	mistVersion := r.Header.Get("X-Version")
	if mistVersion == "" {
		mistVersion = r.UserAgent()
	}
	glog.V(model.VERBOSE).Infof("Got request (%s) mist=%s (%d lines): `%s`", trigger, mistVersion, len(lines), strings.Join(lines, `\n`))
	// glog.V(model.VERBOSE).Infof("User agent: %s", r.UserAgent())
	// glog.V(model.VERBOSE).Infof("Mist version: %s", r.Header.Get("X-Version"))
	started := time.Now()
	doLogRequestEnd := false
	defer func(s time.Time, t string) {
		if doLogRequestEnd {
			took := time.Since(s)
			glog.V(model.VERBOSE).Infof("Request %s ended in %s", t, took)
			metrics.TriggerDuration(t, took)
		}
	}(started, trigger)

	switch trigger {
	case "DEFAULT_STREAM":
		doLogRequestEnd = mc.triggerDefaultStream(w, r, lines, trigger)
	case "LIVE_BANDWIDTH":
		doLogRequestEnd = mc.triggerLiveBandwidth(w, r)
	case "CONN_CLOSE":
		doLogRequestEnd = mc.triggerConnClose(w, r, lines, bs)
	case "PUSH_REWRITE":
		doLogRequestEnd = mc.triggerPushRewrite(w, r, lines, bs)
	case "LIVE_TRACK_LIST":
		doLogRequestEnd = mc.triggerLiveTrackList(w, r, lines, bs)
	case "PUSH_OUT_START":
		doLogRequestEnd = mc.triggerPushOutStart(w, r, lines, bs)
	case "PUSH_END":
		doLogRequestEnd = mc.triggerPushEnd(w, r, lines, bs)
	default:
		glog.Errorf("Got unsupported trigger: '%s'", trigger)
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("false"))
	}
}

func (mc *mac) removeInfoDelayed(playbackID string, done chan struct{}) {
	go func() {
		select {
		case <-done:
			return
		case <-time.After(keepStreamAfterEnd):
			mc.removeInfo(playbackID)
		}
	}()
}

func (mc *mac) removeInfo(playbackID string) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.removeInfoLocked(playbackID)
}

// must be called inside mc.mu.Lock
func (mc *mac) removeInfoLocked(playbackID string) {
	if info, ok := mc.streamInfo[playbackID]; ok {
		close(info.done)
		delete(mc.streamInfo, playbackID)
	}
}

func (mc *mac) wildcardPlaybackID(stream *api.Stream) string {
	return mc.baseNameForStream(stream) + "+" + stream.PlaybackID
}

func (mc *mac) baseNameForStream(stream *api.Stream) string {
	baseName := mc.baseStreamName
	if mc.shouldEnableAudio(stream) {
		baseName += audioEnabledStreamSuffix
	}
	return baseName
}

func (mc *mac) shouldEnableAudio(stream *api.Stream) bool {
	audio := false
	if mc.sendAudio == audioAlways {
		audio = true
	} else if mc.sendAudio == audioRecord {
		audio = stream.Record
	}
	return audio
}

func (mc *mac) createMistStream(streamName string, stream *api.Stream, skipTranscoding bool) error {
	if len(stream.Presets) == 0 && len(stream.Profiles) == 0 {
		stream.Presets = append(stream.Presets, "P144p30fps16x9")
	}
	source := ""
	if mc.balancerHost != "" {
		source = fmt.Sprintf("balance:http://%s/?fallback=push://", mc.balancerHost)
	}
	audio := mc.shouldEnableAudio(stream)
	err := mc.mapi.CreateStream(streamName, stream.Presets,
		LivepeerProfiles2MistProfiles(stream.Profiles), "1", mc.lapi.GetServer()+"/api/stream/"+stream.ID, source, mc.mistHardcodedBroadcasters, skipTranscoding, audio, true)
	// err = mc.mapi.CreateStream(streamKey, stream.Presets, LivepeerProfiles2MistProfiles(stream.Profiles), "1", "http://host.docker.internal:3004/api/stream/"+stream.ID)
	return err
}

func (mc *mac) handleHealthcheck(w http.ResponseWriter, r *http.Request) {
	if config, err := mc.mapi.GetConfig(); err != nil || config == nil {
		glog.Errorf("Error getting mist config on healthcheck. err=%q", err)
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (mc *mac) webServerHandlers() *http.ServeMux {
	mux := http.NewServeMux()
	utils.AddPProfHandlers(mux)
	mux.Handle("/metrics", metrics.Exporter)

	mux.HandleFunc("/_healthz", mc.handleHealthcheck)
	mux.HandleFunc("/", mc.handleDefaultStreamTrigger)
	return mux
}

func (mc *mac) StartServer(bindAddr string) error {
	mux := mc.webServerHandlers()
	mc.srv = &http.Server{
		Addr:    bindAddr,
		Handler: mux,
	}
	mc.startSignalHandler()

	glog.Info("Web server listening on ", bindAddr)
	err := mc.srv.ListenAndServe()
	if err == http.ErrServerClosed {
		glog.Infof("Normal shutdown")
	} else {
		glog.Warningf("Server shut down with err=%v", err)
	}
	return err
}

func (mc *mac) addTrigger(triggers mist.TriggersMap, name, ownURI, def, params string, sync bool) bool {
	nt := mist.Trigger{
		Default: def,
		Handler: ownURI,
		Sync:    sync,
		Params:  params,
	}
	pr := triggers[name]
	found := false
	for _, trig := range pr {
		if trig.Default == nt.Default && trig.Handler == nt.Handler && trig.Sync == nt.Sync && trig.Params == params {
			found = true
			break
		}
	}
	if !found {
		pr = append(pr, nt)
		triggers[name] = pr
	}
	return !found
}

func (mc *mac) SetupTriggers(ownURI string) error {
	triggers, err := mc.mapi.GetTriggers()
	if err != nil {
		glog.Error(err)
		return err
	}
	if triggers == nil {
		triggers = make(mist.TriggersMap)
	}
	added := mc.addTrigger(triggers, "PUSH_REWRITE", ownURI, "000reallylongnonexistenstreamnamethatreallyshouldntexist000", "", true)
	// DEFAULT_STREAM needed when using Mist's load balancing
	// added = mc.addTrigger(triggers, "DEFAULT_STREAM", ownURI, "false", "", true) || added
	if mc.checkBandwidth {
		added = mc.addTrigger(triggers, "LIVE_BANDWIDTH", ownURI, "false", "100000", true) || added
	}
	added = mc.addTrigger(triggers, "CONN_CLOSE", ownURI, "", "", false) || added
	added = mc.addTrigger(triggers, "LIVE_TRACK_LIST", ownURI, "", "", false) || added
	added = mc.addTrigger(triggers, "PUSH_OUT_START", ownURI, "", "", false) || added
	added = mc.addTrigger(triggers, "PUSH_END", ownURI, "", "", false) || added
	if added {
		err = mc.mapi.SetTriggers(triggers)
	}
	// setup base stream if needed
	if mc.baseStreamName != "" {
		apiURL := mc.lapi.GetServer() + "/api/stream/" + mc.baseStreamName
		presets := []string{"P144p30fps16x9"}
		// base stream created with audio disabled
		err = mc.mapi.CreateStream(mc.baseStreamName, presets, nil, "1", apiURL, mc.mistStreamSource, mc.mistHardcodedBroadcasters, false, false, false)
		if err != nil {
			glog.Error(err)
			return err
		}
		// create second stream with audio enabled - used for stream with recording enabled
		err = mc.mapi.CreateStream(mc.baseStreamName+audioEnabledStreamSuffix, presets, nil, "1", apiURL, mc.mistStreamSource, mc.mistHardcodedBroadcasters, false, true, false)
	}
	return err
}

func (mc *mac) startMultistream(wildcardPlaybackID, playbackID string, info *streamInfo) {
	for i := range info.stream.Multistream.Targets {
		go func(targetRef api.MultistreamTargetRef) {
			glog.Infof("==> starting multistream %s", targetRef.ID)
			target, pushURL, err := mc.getPushUrl(info.stream, &targetRef)
			if err != nil {
				glog.Errorf("Error building multistream target push URL. targetId=%s stream=%s err=%v",
					targetRef.ID, wildcardPlaybackID, err)
				return
			} else if target.Disabled {
				glog.Infof("Ignoring disabled multistream target. targetId=%s stream=%s",
					targetRef.ID, wildcardPlaybackID)
				return
			}

			info.mu.Lock()
			info.pushStatus[pushURL] = &pushStatus{
				target:  target,
				profile: targetRef.Profile,
				metrics: &data.MultistreamMetrics{},
			}
			info.mu.Unlock()

			err = mc.mapi.StartPush(wildcardPlaybackID, pushURL)
			if err != nil {
				glog.Errorf("Error starting multistream to target. targetId=%s stream=%s err=%v", targetRef.ID, wildcardPlaybackID, err)
				info.mu.Lock()
				delete(info.pushStatus, pushURL)
				info.mu.Unlock()
				return
			}
			glog.Infof("Started multistream to target. targetId=%s stream=%s url=%s", wildcardPlaybackID, targetRef.ID, pushURL)
		}(info.stream.Multistream.Targets[i])
	}
}

func (mc *mac) startSignalHandler() {
	exitc := make(chan os.Signal, 1)
	signal.Notify(exitc, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		gotSig := <-exitc
		switch gotSig {
		case syscall.SIGINT:
			glog.Infof("Got Ctrl-C, shutting down")
		case syscall.SIGTERM:
			glog.Infof("Got SIGTERM, shutting down")
		default:
			glog.Infof("Got signal %d, shutting down", gotSig)
		}
		mc.shutdown()
	}()
}

func (mc *mac) getPushUrl(stream *api.Stream, targetRef *api.MultistreamTargetRef) (*api.MultistreamTarget, string, error) {
	target, err := mc.lapi.GetMultistreamTarget(targetRef.ID)
	if err != nil {
		return nil, "", fmt.Errorf("error fetching multistream target %s: %w", targetRef.ID, err)
	}
	// Find the actual parameters of the profile we're using
	var videoSelector string
	// Not actually the source. But the highest quality.
	if targetRef.Profile == "source" {
		videoSelector = "maxbps"
	} else {
		var prof *api.Profile
		for _, p := range stream.Profiles {
			if p.Name == targetRef.Profile {
				prof = &p
				break
			}
		}
		if prof == nil {
			return nil, "", fmt.Errorf("profile not found: %s", targetRef.Profile)
		}
		videoSelector = fmt.Sprintf("~%dx%d", prof.Width, prof.Height)
	}
	join := "?"
	if strings.Contains(target.URL, "?") {
		join = "&"
	}
	audioSelector := "maxbps"
	if targetRef.VideoOnly {
		audioSelector = "silent"
	}
	// Inject ?video=~widthxheight to send the correct rendition
	return target, fmt.Sprintf("%s%svideo=%s&audio=%s", target.URL, join, videoSelector, audioSelector), nil
}

func (mc *mac) shutdown() {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	err := mc.srv.Shutdown(ctx)
	glog.Infof("Done shutting down server with err=%v", err)

	mc.cancel()
	mc.srvShutCh <- err
}

func (mc *mac) getStreamInfoLogged(playbackID string) (*streamInfo, bool) {
	info, err := mc.getStreamInfo(playbackID)
	if err != nil {
		glog.Errorf("Error getting stream info playbackID=%q err=%q", playbackID, err)
		return nil, false
	}
	return info, true
}

func (mc *mac) getStreamInfo(playbackID string) (*streamInfo, error) {
	playbackID = mistStreamName2playbackID(playbackID)

	mc.mu.RLock()
	info := mc.streamInfo[playbackID]
	mc.mu.RUnlock()

	if info != nil {
		return info, nil
	}

	glog.Infof("getStreamInfo: Fetching stream not found in memory. playbackID=%s", playbackID)
	stream, err := mc.lapi.GetStreamByPlaybackID(playbackID)
	if err != nil {
		return nil, fmt.Errorf("error getting stream by playback ID %s: %w", playbackID, err)
	}

	pushes := make(map[string]*pushStatus)
	for _, ref := range stream.Multistream.Targets {
		target, pushURL, err := mc.getPushUrl(stream, &ref)
		if err != nil {
			return nil, err
		}
		pushes[pushURL] = &pushStatus{
			target:  target,
			profile: ref.Profile,
			// Assume setup was all successful
			pushStartEmitted: true,
		}
	}

	info = &streamInfo{
		id:         stream.ID,
		stream:     stream,
		done:       make(chan struct{}),
		pushStatus: pushes,
		// Assume setup was all successful
		multistreamStarted: true,
	}
	glog.Infof("getStreamInfo: Created info lazily for stream. playbackID=%s id=%s numPushes=%d", playbackID, stream.ID, len(pushes))

	mc.mu.Lock()
	mc.streamInfo[playbackID] = info
	mc.mu.Unlock()

	return info, nil
}

func (mc *mac) SrvShutCh() chan error {
	return mc.srvShutCh
}

func (tl *trackList) CountVideoTracks() int {
	res := 0
	for _, td := range *tl {
		if td.Type == "video" {
			res++
		}
	}
	return res
}

func mistStreamName2playbackID(msn string) string {
	if strings.Contains(msn, "+") {
		return strings.Split(msn, "+")[1]
	}
	return msn
}

func pushToMultistreamTargetInfo(pushInfo *pushStatus) data.MultistreamTargetInfo {
	return data.MultistreamTargetInfo{
		ID:      pushInfo.target.ID,
		Name:    pushInfo.target.Name,
		Profile: pushInfo.profile,
	}
}
