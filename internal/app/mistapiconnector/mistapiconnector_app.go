package mistapiconnector

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/golang/glog"
	"github.com/google/uuid"
	"github.com/livepeer/stream-tester/apis/consul"
	"github.com/livepeer/stream-tester/apis/livepeer"
	"github.com/livepeer/stream-tester/apis/mist"
	mistapi "github.com/livepeer/stream-tester/apis/mist"
	"github.com/livepeer/stream-tester/internal/metrics"
	"github.com/livepeer/stream-tester/internal/utils"
	"github.com/livepeer/stream-tester/model"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"google.golang.org/grpc"
)

const streamPlaybackPrefix = "playback_"
const traefikRuleTemplate = "HostRegexp(`%s`) && PathPrefix(`/hls/%s/`)"
const traefikRuleTemplateDouble = "HostRegexp(`%s`) && (PathPrefix(`/hls/%s/`) || PathPrefix(`/hls/%s/`))"
const traefikKeyPathRouters = `traefik/http/routers/`
const traefikKeyPathServices = `traefik/http/services/`
const traefikKeyPathMiddlewares = `traefik/http/middlewares/`
const audioAlways = "always"
const audioNever = "never"
const audioRecord = "record"
const audioEnabledStreamSuffix = "rec"
const etcdDialTimeout = 5 * time.Second
const etcdAutoSyncInterval = 5 * time.Minute
const etcdSessionTTL = 10 // in seconds
const etcdSessionRecoverBackoff = 3 * time.Second
const etcdSessionRecoverTimeout = 2 * time.Minute
const waitForPushError = 7 * time.Second
const keepStreamAfterEnd = 15 * time.Second
const QUEUE_NAME = "webhook_default_queue"
const eventRTMPPushConnected = "rtmp.push.connected"
const eventRTMPPushError = "rtmp.push.error"
const eventRTMPPushDisconnected = "rtmp.push.disconnected"

// const EXCHANGE_NAME = "webhook_default_exchange"

type (
	// IMac creates new Mist API Connector application
	IMac interface {
		SetupTriggers(ownURI string) error
		StartServer(bindAddr string) error
		SrvShutCh() chan error
	}

	// MacOptions configuration object
	MacOptions struct {
		Host      string
		Port      uint32
		OwnURI    string
		MistHost  string
		MistCreds string
		APIToken  string
		APIServer string
	}

	etcdRevData struct {
		revision int64
		entries  []string
	}

	pushStatus struct {
		pushStartEmitted bool
		pushStopped      bool
		pushInfo         *livepeer.PushTarget
	}

	streamInfo struct {
		id              string
		stopped         bool
		rtmpPushStarted bool
		stream          *livepeer.CreateStreamResp
		done            chan struct{}
		mu              sync.Mutex
		pushStatus      map[string]*pushStatus
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

	trackList map[string]*trackListDesc

	mac struct {
		ctx            context.Context
		cancel         context.CancelFunc
		opts           *MacOptions
		mapi           *mist.API
		lapi           *livepeer.API
		balancerHost   string
		srv            *http.Server
		srvShutCh      chan error
		mu             sync.RWMutex
		mistHot        string
		checkBandwidth bool
		consulURL      *url.URL
		consulPrefix   string
		mistURL        string
		playbackDomain string
		sendAudio      string
		baseStreamName string
		useEtcd        bool
		etcdClient     *clientv3.Client
		etcdSession    *concurrency.Session
		etcdPub2rev    map[string]etcdRevData // public key to revision of etcd keys
		pub2info       map[string]*streamInfo // public key to info
		producer       Producer
		// pub2id         map[string]string // public key to stream id
	}
)

// NewMac ...
func NewMac(mistHost string, mapi *mist.API, lapi *livepeer.API, balancerHost string, checkBandwidth bool, consul *url.URL, consulPrefix, playbackDomain, mistURL,
	sendAudio, baseStreamName string, etcdEndpoints []string, etcdCaCert, etcdCert, etcdKey, amqpUrl string) (IMac, error) {
	if balancerHost != "" && !strings.Contains(balancerHost, ":") {
		balancerHost = balancerHost + ":8042" // must set default port for Mist's Load Balancer
	}
	useEtcd := false
	var cli *clientv3.Client
	var sess *concurrency.Session
	var err error
	ctx, cancel := context.WithCancel(context.Background())

	/*
		conn, err := amqp.Dial("amqp://localhost:5672/livepeer")
		if err != nil {
			err = fmt.Errorf("mist-api-connector: Failed to connect to RabbitMQ err=%w", err)
			return nil, err
		}
		ch, err := conn.Channel()
		if err != nil {
			conn.Close()
			err = fmt.Errorf("mist-api-connector: Failed to open channel err=%w", err)
			return nil, err
		}
		if err := ch.Confirm(false); err != nil {
			conn.Close()
			return nil, fmt.Errorf("request confirms: %w", err)
		}
		confirms := make(chan amqp.Confirmation, 16)
		closed := make(chan *amqp.Error, 16)
		ch.NotifyPublish(confirms)
		ch.NotifyClose(closed)
		go func() {
			for confirm := range confirms {
				glog.Infof("==> got confirm ack=%v tag=%v", confirm.Ack, confirm.DeliveryTag)
			}
		}()
		go func() {
			for close := range closed {
				glog.Infof("==> got amqp closed %+v", close)
			}
		}()
	*/

	var producer Producer
	if amqpUrl != "" {
		pu, err := url.Parse(amqpUrl)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("error parsing AMQP url err=%w", err)
		}

		glog.Infof("Creating AMQP producer with url=%s", pu.Redacted())
		producer, err = NewAMQPProducer(ctx, amqpUrl, "", QUEUE_NAME, "")
		if err != nil {
			cancel()
			return nil, err
		}
	} else {
		glog.Infof("AMQP url is empty!")
	}

	glog.Infof("etcd endpoints: %+v, len %d", etcdEndpoints, len(etcdEndpoints))
	if len(etcdEndpoints) > 0 {
		var tcfg *tls.Config
		if etcdCaCert != "" || etcdCert != "" || etcdKey != "" {
			tlsifo := transport.TLSInfo{
				CertFile:      etcdCert,
				KeyFile:       etcdKey,
				TrustedCAFile: etcdCaCert,
			}
			tcfg, err = tlsifo.ClientConfig()
			if err != nil {
				cancel()
				return nil, err
			}
		}
		useEtcd = true
		cli, err = clientv3.New(clientv3.Config{
			Endpoints:        etcdEndpoints,
			DialTimeout:      etcdDialTimeout,
			AutoSyncInterval: etcdAutoSyncInterval,
			TLS:              tcfg,
			DialOptions:      []grpc.DialOption{grpc.WithBlock()},
		})
		if err != nil {
			err = fmt.Errorf("mist-api-connector: Error connecting etcd err=%w", err)
			cancel()
			return nil, err
		}
		ctx, cancel := context.WithTimeout(context.Background(), etcdDialTimeout)
		err = cli.Sync(ctx)
		cancel()
		if err != nil {
			err = fmt.Errorf("mist-api-connector: Error syncing etcd endpoints err=%w", err)
			return nil, err
		}
		sess, err = newEtcdSession(cli)
		if err != nil {
			return nil, err
		}
	}
	mc := &mac{
		mistHot:        mistHost,
		mapi:           mapi,
		lapi:           lapi,
		checkBandwidth: checkBandwidth,
		balancerHost:   balancerHost,
		// pub2id:         make(map[string]string), // public key to stream id
		pub2info:       make(map[string]*streamInfo), // public key to info
		consulURL:      consul,
		consulPrefix:   consulPrefix,
		mistURL:        mistURL,
		playbackDomain: playbackDomain,
		sendAudio:      sendAudio,
		baseStreamName: baseStreamName,
		useEtcd:        useEtcd,
		etcdClient:     cli,
		etcdSession:    sess,
		etcdPub2rev:    make(map[string]etcdRevData), // public key to revision of etcd keys
		srvShutCh:      make(chan error),
		ctx:            ctx,
		cancel:         cancel,
		producer:       producer,
	}
	go mc.recoverSessionLoop()
	return mc, nil
}

// LivepeerProfiles2MistProfiles converts Livepeer's API profiles to Mist's ones
func LivepeerProfiles2MistProfiles(lps []livepeer.Profile) []mist.Profile {
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
	if lines[2] == "RTMP" && len(lines[3]) > 0 { // PUSH_END sends CONN_CLOSE too, but it has empty last line
		playbackID := strings.TrimPrefix(lines[0], streamPlaybackPrefix)
		if mc.baseStreamName != "" && strings.Contains(playbackID, "+") {
			playbackID = strings.Split(playbackID, "+")[1]
		}
		mc.mu.Lock()
		if info, has := mc.pub2info[playbackID]; has {
			glog.Infof("Setting stream's manifestID=%s playbackID=%s active status to false", info.id, playbackID)
			if mc.consulURL != nil {
				consulPlaybackID := mc.consulPrefix + playbackID
				go consul.DeleteKey(mc.consulURL, traefikKeyPathRouters+consulPlaybackID, true)
				// shouldn't exists with new scheme, but keeping here to clean up routes made with old scheme
				go consul.DeleteKey(mc.consulURL, traefikKeyPathServices+consulPlaybackID, true)
				if mc.baseStreamName != "" {
					go consul.DeleteKey(mc.consulURL, traefikKeyPathMiddlewares+consulPlaybackID, true)
				}
			}
			_, err := mc.lapi.SetActive(info.id, false)
			if err != nil {
				glog.Error(err)
			}
			info.mu.Lock()
			info.stopped = true
			info.mu.Unlock()
			go mc.removeInfoAfter(playbackID, info)
			metrics.StopStream(true)
		}
		mc.mu.Unlock()
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
				// check if stream is in our map of currently playing streams
				mc.mu.Lock()
				defer mc.mu.Unlock() // hold the lock until exit so that trigger to RTMP_REWRITE can't create
				// another Mist stream in the same time
				if info, has := mc.pub2info[playbackID]; has {
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
	// 1. When in RTMP_PUSH_REWRITE we got request for unknown stream and thus
	//    haven't created new stream in Mist
	// 2. When someone pulls HLS for stream that exists but is not active (no
	//    RTMP stream coming in).
	w.WriteHeader(http.StatusForbidden)
	w.Write([]byte("false"))
	return true
}

func (mc *mac) triggerRtmpPushRewrite(w http.ResponseWriter, r *http.Request, lines []string, rawRequest string) bool {
	if len(lines) < 2 {
		glog.Errorf("Expected 2 lines, got %d, request \n%s", len(lines), rawRequest)
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("false"))
		return false
	}
	// glog.V(model.INSANE).Infof("Parsed request (%d):\n%+v", len(lines), lines)
	pu, err := url.Parse(lines[0])
	responseURL := lines[0]
	if err != nil {
		glog.Errorf("Error parsing url=%s err=%v", lines[0], err)
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("false"))
		return false
	}
	pp := strings.Split(pu.Path, "/")
	if len(pp) != 3 {
		glog.Errorf("URL wrongly formatted - should be in format rtmp://mist.host/live/streamKey")
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("false"))
		return false
	}
	streamKey := pp[2]
	glog.V(model.VVERBOSE).Infof("Requested stream key is '%s'", streamKey)
	// ask API
	stream, err := mc.lapi.GetStreamByKey(streamKey)
	if err != nil || stream == nil {
		glog.Errorf("Error getting stream info from Livepeer API streamKey=%s err=%v", streamKey, err)
		/*
			if err == livepeer.ErrNotExists {
				// mc.mapi.DeleteStreams(streamKey)
				w.Write([]byte(lines[0]))
			} else {
				w.WriteHeader(http.StatusNotFound)
			}
		*/
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
		defer mc.mu.Unlock()
		if info, has := mc.pub2info[stream.PlaybackID]; has {
			info.mu.Lock()
			streamStopped := info.stopped
			info.mu.Unlock()
			glog.Infof("Stream playbackID=%s stopped=%v already in map, removing its info", stream.PlaybackID, streamStopped)
			mc.removeInfo(stream.PlaybackID)
		}
		mc.pub2info[stream.PlaybackID] = &streamInfo{
			id:         stream.ID,
			stream:     stream,
			done:       make(chan struct{}),
			pushStatus: make(map[string]*pushStatus),
		}
		streamKey = stream.PlaybackID
		// streamKey = strings.ReplaceAll(streamKey, "-", "")
		if mc.balancerHost != "" {
			streamKey = streamPlaybackPrefix + streamKey
		}
		if mc.baseStreamName == "" {
			pp[2] = streamKey
		} else {
			pp[2] = mc.wildcardPlaybackID(stream)
		}
		pu.Path = strings.Join(pp, "/")
		responseURL = pu.String()
		ok, err := mc.lapi.SetActive(stream.ID, true)
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
	if mc.consulURL != nil {
		// now create routing rule in the Consul for HLS playback
		if mc.baseStreamName != "" {
			wildcardPlaybackID := mc.wildcardPlaybackID(stream)
			playbackID := mc.consulPrefix + stream.PlaybackID
			serviceName := mc.consulPrefix + serviceNameFromMistURL(mc.mistURL)
			err = consul.PutKeysWithCurrentTimeRetry(
				4,
				mc.consulURL,
				traefikKeyPathRouters+playbackID+"/rule",
				fmt.Sprintf(traefikRuleTemplateDouble, mc.playbackDomain, stream.PlaybackID, wildcardPlaybackID),
				traefikKeyPathRouters+playbackID+"/service",
				serviceName,
				traefikKeyPathRouters+playbackID+"/middlewares/0",
				playbackID+"-1",
				traefikKeyPathRouters+playbackID+"/middlewares/1",
				playbackID+"-2",

				traefikKeyPathMiddlewares+playbackID+"-1/stripprefix/prefixes/0",
				`/hls/`+stream.PlaybackID,
				traefikKeyPathMiddlewares+playbackID+"-1/stripprefix/prefixes/1",
				`/hls/`+wildcardPlaybackID,
				traefikKeyPathMiddlewares+playbackID+"-2/addprefix/prefix",
				`/hls/`+wildcardPlaybackID,

				// traefikKeyPathMiddlewares+playbackID+"/replacepathregex/regex",
				// fmt.Sprintf(`^/hls/%s\+(.*)`, mc.baseNameForStream(stream)),
				// traefikKeyPathMiddlewares+playbackID+"/replacepathregex/replacement",
				// `/hls/$1`,

				traefikKeyPathServices+serviceName+"/loadbalancer/servers/0/url",
				mc.mistURL,
				traefikKeyPathServices+serviceName+"/loadbalancer/passhostheader",
				"false",
			)
		} else {
			err = consul.PutKeys(
				mc.consulURL,
				traefikKeyPathRouters+streamKey+"/rule",
				fmt.Sprintf(traefikRuleTemplate, mc.playbackDomain, streamKey),
				traefikKeyPathRouters+streamKey+"/service",
				streamKey,
				traefikKeyPathServices+streamKey+"/loadbalancer/servers/0/url",
				mc.mistURL,
				traefikKeyPathServices+streamKey+"/loadbalancer/passhostheader",
				"false",
			)
		}
		if err != nil {
			glog.Errorf("Error creating Traefik rule err=%v", err)
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("false"))
			return true
		}
	}
	if mc.useEtcd {
		// now create routing rule in the etcd for HLS playback
		if mc.baseStreamName != "" {
			wildcardPlaybackID := mc.wildcardPlaybackID(stream)
			playbackID := mc.consulPrefix + stream.PlaybackID
			serviceName := mc.consulPrefix + serviceNameFromMistURL(mc.mistURL)
			err = mc.putEtcdKeys(mc.etcdSession, stream.PlaybackID,
				traefikKeyPathRouters+playbackID+"/rule",
				fmt.Sprintf(traefikRuleTemplateDouble, mc.playbackDomain, stream.PlaybackID, wildcardPlaybackID),
				traefikKeyPathRouters+playbackID+"/service",
				serviceName,
				traefikKeyPathRouters+playbackID+"/middlewares/0",
				playbackID+"-1",
				traefikKeyPathRouters+playbackID+"/middlewares/1",
				playbackID+"-2",

				traefikKeyPathMiddlewares+playbackID+"-1/stripprefix/prefixes/0",
				`/hls/`+stream.PlaybackID,
				traefikKeyPathMiddlewares+playbackID+"-1/stripprefix/prefixes/1",
				`/hls/`+wildcardPlaybackID,
				traefikKeyPathMiddlewares+playbackID+"-2/addprefix/prefix",
				`/hls/`+wildcardPlaybackID,

				traefikKeyPathServices+serviceName+"/loadbalancer/servers/0/url",
				mc.mistURL,
				traefikKeyPathServices+serviceName+"/loadbalancer/passhostheader",
				"false",
			)
		} else {
			err = mc.putEtcdKeys(mc.etcdSession,
				stream.PlaybackID,
				traefikKeyPathRouters+streamKey+"/rule",
				fmt.Sprintf(traefikRuleTemplate, mc.playbackDomain, streamKey),
				traefikKeyPathRouters+streamKey+"/service",
				streamKey,
				traefikKeyPathServices+streamKey+"/loadbalancer/servers/0/url",
				mc.mistURL,
				traefikKeyPathServices+streamKey+"/loadbalancer/passhostheader",
				"false",
			)
		}
		if err != nil {
			glog.Errorf("Error creating etcd Traefik rule for playbackID=%s streamID=%s err=%v", stream.PlaybackID, stream.ID, err)
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("false"))
			return true
		}
	}
	w.Write([]byte(responseURL))
	metrics.StartStream()
	glog.Infof("Responded with '%s'", responseURL)
	// mc.startPushTargets(stream)
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
		videoTracksNum := tl.CoundVideoTracks()
		playbackID := mistStreamName2playbackID(lines[0])
		glog.Infof("for video %s got %d video tracks", playbackID, videoTracksNum)
		glog.Infof("===> pub %+v", mc.pub2info)
		mc.mu.RLock()
		defer mc.mu.RUnlock()
		if info, ok := mc.pub2info[playbackID]; ok {
			if len(info.stream.PushTargets) > 0 && !info.rtmpPushStarted && videoTracksNum > 1 {
				info.rtmpPushStarted = true
				mc.startPushTargets(lines[0], playbackID, info)
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
		// glog.Infof("for video %s got %d video tracks", playbackID, videoTracksNum)
		glog.Infof("===> pub %+v", mc.pub2info)
		mc.mu.RLock()
		defer mc.mu.RUnlock()
		if info, ok := mc.pub2info[playbackID]; ok {
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
			// there was no error starting RTMP push, so no we can send 'rtmp.push.connected' webhook event
			pushInfo.pushStartEmitted = true
			mc.emitWebhookEvent(info, pushInfo, eventRTMPPushConnected)
		}
	}
}

type WebhookMessage struct {
	ID        string                 `json:"id,omitempty"`
	Event     string                 `json:"event,omitempty"`
	CreatedAt int64                  `json:"createdAt,omitempty"`
	UserID    string                 `json:"userId,omitempty"`
	StreamID  string                 `json:"streamId,omitempty"`
	Payload   map[string]interface{} `json:"payload,omitempty"`
}

func (mc *mac) emitWebhookEvent(info *streamInfo, pushInfo *pushStatus, event string) {
	if mc.producer == nil {
		return
	}
	wm := WebhookMessage{
		ID:        uuid.NewString(),
		Event:     event,
		CreatedAt: time.Now().UnixNano() / int64(time.Millisecond),
		UserID:    info.stream.UserID,
		StreamID:  info.stream.ID,
		Payload:   map[string]interface{}{"pushUrl": pushInfo.pushInfo.URL},
	}
	err := mc.producer.Publish(mc.ctx, QUEUE_NAME, &wm)
	if err != nil {
		glog.Errorf("Error publishing message msg=%+v err=%v", wm, err)
		return
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
		glog.Infof("===> pub %+v", mc.pub2info)
		mc.mu.RLock()
		defer mc.mu.RUnlock()
		if info, ok := mc.pub2info[playbackID]; ok {
			info.mu.Lock()
			defer info.mu.Unlock()
			if pushInfo, ok := info.pushStatus[lines[2]]; ok {
				if pushInfo.pushStartEmitted {
					// emit normal push.end
					mc.emitWebhookEvent(info, pushInfo, eventRTMPPushDisconnected)
				} else {
					pushInfo.pushStopped = true
					//  emit push error
					mc.emitWebhookEvent(info, pushInfo, eventRTMPPushError)
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
	b, err := ioutil.ReadAll(r.Body)
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
	case "RTMP_PUSH_REWRITE":
		doLogRequestEnd = mc.triggerRtmpPushRewrite(w, r, lines, bs)
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

func (mc *mac) removeInfoAfter(playbackID string, info *streamInfo) {
	select {
	case <-info.done:
	case <-time.After(keepStreamAfterEnd):
	}
	mc.mu.Lock()
	mc.removeInfo(playbackID)
	mc.mu.Unlock()
}

// must be called inside mu.Lock
func (mc *mac) removeInfo(playbackID string) {
	if info, ok := mc.pub2info[playbackID]; ok {
		close(info.done)
		delete(mc.pub2info, playbackID)
		if mc.useEtcd {
			mc.deleteEtcdKeys(playbackID)
		}
	}
}

// putEtcdKeys puts keys in one transaction
func (mc *mac) putEtcdKeys(sess *concurrency.Session, playbackID string, kvs ...string) error {
	if len(kvs) == 0 || len(kvs)%2 != 0 {
		return errors.New("number of arguments should be even")
	}
	cmp := clientv3.Compare(clientv3.CreateRevision(kvs[0]), ">", -1) // basically noop - will always be true
	thn := make([]clientv3.Op, 0, len(kvs)/2)
	get := clientv3.OpGet(kvs[0])
	for i := 0; i < len(kvs); i += 2 {
		thn = append(thn, clientv3.OpPut(kvs[i], kvs[i+1], clientv3.WithLease(sess.Lease())))
	}
	ctx, cancel := context.WithTimeout(context.Background(), etcdDialTimeout)
	resp, err := mc.etcdClient.Txn(ctx).If(cmp).Then(thn...).Else(get).Commit()
	cancel()
	if err != nil {
		glog.Errorf("mist-api-connector: error putting keys for playbackID=%s err=%v", playbackID, err)
		return err
	}
	if !resp.Succeeded {
		panic("unexpected")
	}
	glog.Infof("for playbackID=%s created %d keys in etcd revision=%d", playbackID, len(kvs)/2, resp.Header.Revision)
	mc.etcdPub2rev[playbackID] = etcdRevData{resp.Header.Revision, kvs}
	return nil
}

func (mc *mac) deleteEtcdKeys(playbackID string) {
	etcdPlaybackID := mc.consulPrefix + playbackID
	if rev, ok := mc.etcdPub2rev[playbackID]; ok {
		pathKey := traefikKeyPathRouters + etcdPlaybackID
		ruleKey := pathKey + "/rule"
		cmp := clientv3.Compare(clientv3.ModRevision(ruleKey), "=", rev.revision)
		ctx, cancel := context.WithTimeout(context.Background(), etcdDialTimeout)
		thn := []clientv3.Op{
			clientv3.OpDelete(pathKey, clientv3.WithRange(pathKey+"~")),
		}
		if mc.baseStreamName != "" {
			middleWaresKey := traefikKeyPathMiddlewares + etcdPlaybackID
			thn = append(thn,
				clientv3.OpDelete(middleWaresKey, clientv3.WithRange(middleWaresKey+"~")),
			)
		}
		get := clientv3.OpGet(ruleKey)
		resp, err := mc.etcdClient.Txn(ctx).If(cmp).Then(thn...).Else(get).Commit()
		cancel()
		delete(mc.etcdPub2rev, playbackID)
		if err != nil {
			glog.Errorf("mist-api-connector: error deleting keys for playbackID=%s err=%v", playbackID, err)
		}
		if resp.Succeeded {
			glog.Errorf("mist-api-connector: success deleting keys for playbackID=%s rev=%d", playbackID, rev.revision)
		} else {
			var curRev int64
			if len(resp.Responses) > 0 && len(resp.Responses[0].GetResponseRange().Kvs) > 0 {
				curRev = resp.Responses[0].GetResponseRange().Kvs[0].CreateRevision
			}
			glog.Errorf("mist-api-connector: unsuccessful deleting keys for playbackID=%s myRev=%d curRev=%d pathKey=%s",
				playbackID, rev.revision, curRev, pathKey)
		}
	} else {
		glog.Errorf("mist-api-connector: etcd revision for stream playbackID=%s not found", playbackID)
	}
}

func (mc *mac) recoverSessionLoop() {
	clientCtx := mc.etcdClient.Ctx()
	for clientCtx.Err() == nil {
		select {
		case <-clientCtx.Done():
			// client closed, which means app shutted down
			return
		case <-mc.etcdSession.Done():
		}
		glog.Infof("etcd session with lease=%d is lost, trying to recover", mc.etcdSession.Lease())

		ctx, cancel := context.WithTimeout(clientCtx, etcdSessionRecoverTimeout)
		err := mc.recoverEtcdSession(ctx)
		cancel()

		if err != nil && clientCtx.Err() == nil {
			glog.Errorf("mist-api-connector: unrecoverable etcd session. err=%q.", err)
			return
		}
	}
}

func (mc *mac) recoverEtcdSession(ctx context.Context) error {
	for {
		err := mc.recoverEtcdSessionOnce()
		if err == nil {
			return nil
		}

		select {
		case <-time.After(etcdSessionRecoverBackoff):
			glog.Errorf("mist-api-connector: Retrying etcd session recover. err=%q", err)
			continue
		case <-ctx.Done():
			return fmt.Errorf("mist-api-connector: Timeout recovering etcd session err=%w", err)
		}
	}
}

func (mc *mac) recoverEtcdSessionOnce() error {
	sess, err := newEtcdSession(mc.etcdClient)
	if err != nil {
		return err
	}

	mc.mu.Lock()
	defer mc.mu.Unlock()
	for playbackId, rev := range mc.etcdPub2rev {
		err := mc.putEtcdKeys(sess, playbackId, rev.entries...)
		if err != nil {
			sess.Close()
			return fmt.Errorf("mist-api-connector: Error re-creating etcd keys. playbackId=%q, err=%w", playbackId, err)
		}
	}

	mc.etcdSession.Close()
	mc.etcdSession = sess
	glog.Infof("Recovered etcd session. lease=%d", sess.Lease())
	return nil
}

func newEtcdSession(etcdClient *clientv3.Client) (*concurrency.Session, error) {
	glog.Infof("Starting new etcd session ttl=%d", etcdSessionTTL)
	sess, err := concurrency.NewSession(etcdClient, concurrency.WithTTL(etcdSessionTTL))
	if err != nil {
		glog.Errorf("Failed to start etcd session err=%q", err)
		return nil, fmt.Errorf("mist-api-connector: Error creating etcd session err=%w", err)
	}
	glog.Infof("etcd got lease %d", sess.Lease())
	return sess, nil
}

func (mc *mac) wildcardPlaybackID(stream *livepeer.CreateStreamResp) string {
	return mc.baseNameForStream(stream) + "+" + stream.PlaybackID
}

func (mc *mac) baseNameForStream(stream *livepeer.CreateStreamResp) string {
	baseName := mc.baseStreamName
	if mc.shouldEnableAudio(stream) {
		baseName += audioEnabledStreamSuffix
	}
	return baseName
}

func (mc *mac) shouldEnableAudio(stream *livepeer.CreateStreamResp) bool {
	audio := false
	if mc.sendAudio == audioAlways {
		audio = true
	} else if mc.sendAudio == audioRecord {
		audio = stream.Record
	}
	return audio
}

func (mc *mac) createMistStream(streamName string, stream *livepeer.CreateStreamResp, skipTranscoding bool) error {
	if len(stream.Presets) == 0 && len(stream.Profiles) == 0 {
		stream.Presets = append(stream.Presets, "P144p30fps16x9")
	}
	source := ""
	if mc.balancerHost != "" {
		source = fmt.Sprintf("balance:http://%s/?fallback=push://", mc.balancerHost)
	}
	audio := mc.shouldEnableAudio(stream)
	err := mc.mapi.CreateStream(streamName, stream.Presets,
		LivepeerProfiles2MistProfiles(stream.Profiles), "1", mc.lapi.GetServer()+"/api/stream/"+stream.ID, source, skipTranscoding, audio)
	// err = mc.mapi.CreateStream(streamKey, stream.Presets, LivepeerProfiles2MistProfiles(stream.Profiles), "1", "http://host.docker.internal:3004/api/stream/"+stream.ID)
	return err
}

func (mc *mac) webServerHandlers() *http.ServeMux {
	mux := http.NewServeMux()
	utils.AddPProfHandlers(mux)
	// mux.Handle("/metrics", utils.InitPrometheusExporter("mistconnector"))
	mux.Handle("/metrics", metrics.Exporter)

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

func (mc *mac) addTrigger(triggers mistapi.TriggersMap, name, ownURI, def, params string, sync bool) bool {
	nt := mistapi.Trigger{
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
		triggers = make(mistapi.TriggersMap)
	}
	added := mc.addTrigger(triggers, "RTMP_PUSH_REWRITE", ownURI, "000reallylongnonexistenstreamnamethatreallyshouldntexist000", "", true)
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
		err = mc.mapi.CreateStream(mc.baseStreamName, presets, nil, "1", apiURL, "", false, false)
		if err != nil {
			glog.Error(err)
			return err
		}
		// create second stream with audio enabled - used for stream with recording enabled
		err = mc.mapi.CreateStream(mc.baseStreamName+audioEnabledStreamSuffix, presets, nil, "1", apiURL, "", false, true)
	}
	return err
}

func serviceNameFromMistURL(murl string) string {
	murl = strings.TrimPrefix(murl, "https://")
	murl = strings.TrimPrefix(murl, "http://")
	murl = strings.ReplaceAll(murl, ".", "-")
	murl = strings.ReplaceAll(murl, "/", "-")
	return murl
}

func (mc *mac) startPushTargets(wildcardPlaybackID, playbackID string, info *streamInfo) {
	for _, target := range info.stream.PushTargets {
		go func(target livepeer.StreamPushTarget) {
			glog.Infof("==> starting push %s", target.ID)
			pushTarget, err := mc.lapi.GetPushTargetR(target.ID)
			if err != nil {
				glog.Errorf("Error downloading PushTarget pushTargetId=%s stream=%s err=%v",
					target.ID, wildcardPlaybackID, err)
				return
			}
			// Find the actual parameters of the profile we're using
			var videoSelector string
			// Not actually the source. But the highest quality.
			if target.Profile == "source" {
				videoSelector = "maxbps"
			} else {
				var prof *livepeer.Profile
				for _, p := range info.stream.Profiles {
					if p.Name == target.Profile {
						prof = &p
						break
					}
				}
				if prof == nil {
					glog.Errorf("Error starting PushTarget pushTargetId=%s stream=%s err=couldn't find profile %s",
						target.ID, wildcardPlaybackID, target.Profile)
					return
				}
				videoSelector = fmt.Sprintf("~%dx%d", prof.Width, prof.Height)
			}
			join := "?"
			if strings.Contains(pushTarget.URL, "?") {
				join = "&"
			}
			// Inject ?video=~widthxheight to send the correct rendition
			selectorURL := fmt.Sprintf("%s%svideo=%s&audio=maxbps", pushTarget.URL, join, videoSelector)
			info.mu.Lock()
			info.pushStatus[selectorURL] = &pushStatus{
				pushInfo: pushTarget,
			}

			err = mc.mapi.StartPush(wildcardPlaybackID, selectorURL)
			if err != nil {
				glog.Errorf("Error starting PushTarget pushTargetId=%s stream=%s err=%v", target.ID, wildcardPlaybackID, err)
				delete(info.pushStatus, selectorURL)
				info.mu.Unlock()
				return
			}
			glog.Infof("Started PushTarget stream=%s pushTargetId=%s url=%s", wildcardPlaybackID, target.ID, selectorURL)
			info.mu.Unlock()
		}(target)
	}
}

func (mc *mac) startSignalHandler() {
	exitc := make(chan os.Signal, 1)
	// signal.Notify(exitc, syscall.SIGINT, syscall.SIGTERM, os.Interrupt, os.Kill)
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

func (mc *mac) shutdown() {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	err := mc.srv.Shutdown(ctx)
	cancel()
	mc.cancel()
	glog.Infof("Done shutting down server with err=%v", err)
	mc.etcdClient.Close()
	// now call /setactve/false on active connections
	mc.deactiveAllStreams()
	mc.srvShutCh <- err
}

// deactiveAllStreams sends /setactive/false for all the active streams
func (mc *mac) deactiveAllStreams() {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	ids := make([]string, 0, len(mc.pub2info))
	for _, info := range mc.pub2info {
		ids = append(ids, info.id)
	}
	if len(ids) > 0 {
		updated, err := mc.lapi.DeactivateMany(ids)
		if err != nil {
			glog.Errorf("Error setting many isActive to false ids=%+v err=%v", ids, err)
		} else {
			glog.Infof("Set many isActive to false ids=%+v rowCount=%d", ids, updated)
		}
	}
}

func (mc *mac) SrvShutCh() chan error {
	return mc.srvShutCh
}

func (tl *trackList) CoundVideoTracks() int {
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
