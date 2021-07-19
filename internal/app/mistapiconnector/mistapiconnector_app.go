package mistapiconnector

import (
	"context"
	"crypto/tls"
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

	mac struct {
		opts           *MacOptions
		mapi           *mist.API
		lapi           *livepeer.API
		balancerHost   string
		pub2id         map[string]string // public key to stream id
		srv            *http.Server
		srvShutCh      chan error
		mu             sync.Mutex
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
	}
)

// NewMac ...
func NewMac(mistHost string, mapi *mist.API, lapi *livepeer.API, balancerHost string, checkBandwidth bool, consul *url.URL, consulPrefix, playbackDomain, mistURL,
	sendAudio, baseStreamName string, etcdEndpoints []string, etcdCaCert, etcdCert, etcdKey string) (IMac, error) {
	if balancerHost != "" && !strings.Contains(balancerHost, ":") {
		balancerHost = balancerHost + ":8042" // must set default port for Mist's Load Balancer
	}
	useEtcd := false
	var cli *clientv3.Client
	var sess *concurrency.Session
	var err error
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
		pub2id:         make(map[string]string), // public key to stream id
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
	if trigger == "DEFAULT_STREAM" {
		if mc.balancerHost == "" {
			glog.V(model.VERBOSE).Infof("Request %s: (%d lines) responded with forbidden", trigger, len(lines))
			w.WriteHeader(http.StatusForbidden)
			w.Write([]byte("false"))
			return
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
					if _, has := mc.pub2id[playbackID]; has {
						// that means that RTMP stream is currently gets streamed into our Mist node
						// and so no changes needed to the Mist configuration
						glog.Infof("Already in the playing map, returning %s", streamNameInMist)
						w.WriteHeader(http.StatusOK)
						w.Write([]byte(streamNameInMist))
						return
					}

					// check if such stream already exists in Mist's config
					streams, activeStreams, err := mc.mapi.Streams()
					if err != nil {
						glog.Warningf("Error getting streams list from Mist: %v", err)
						w.WriteHeader(http.StatusOK)
						w.Write([]byte(streamNameInMist))
						return
					}
					if utils.StringsSliceContains(activeStreams, streamNameInMist) {
						glog.Infof("Stream is in active map, returning %s", streamNameInMist)
						w.WriteHeader(http.StatusOK)
						w.Write([]byte(streamNameInMist))
						return
					}
					if mstream, has := streams[streamNameInMist]; has {
						if len(mstream.Processes) == 0 {
							// Stream exists and has transcoding turned off
							glog.Infof("Requested stream '%s' already exists in Mist config, just returning it's name", streamNameInMist)
							w.WriteHeader(http.StatusOK)
							w.Write([]byte(streamNameInMist))
							return
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
						return
					}
					glog.V(model.DEBUG).Infof("For stream %s got info %+v", playbackID, stream)
					if stream.Deleted {
						glog.Infof("Stream %s was deleted, so deleting Mist's stream configuration", playbackID)
						go mc.mapi.DeleteStreams(streamNameInMist)
						w.WriteHeader(http.StatusNotFound)
						w.Write([]byte("false"))
						return
					}
					err = mc.createMistStream(streamNameInMist, stream, true)
					if err != nil {
						glog.Errorf("Error creating stream on the Mist server: %v", err)
					}
					w.WriteHeader(http.StatusOK)
					w.Write([]byte(streamNameInMist))
					return
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
		return
	}
	if trigger == "LIVE_BANDWIDTH" {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("yes"))
		return
	}
	if trigger == "CONN_CLOSE" {
		if len(lines) < 3 {
			glog.Errorf("Expected 3 lines, got %d, request \n%s", len(lines), bs)
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("false"))
			return
		}
		if lines[2] == "RTMP" {
			doLogRequestEnd = true
			playbackID := strings.TrimPrefix(lines[0], streamPlaybackPrefix)
			if mc.baseStreamName != "" && strings.Contains(playbackID, "+") {
				playbackID = strings.Split(playbackID, "+")[1]
			}
			mc.mu.Lock()
			if id, has := mc.pub2id[playbackID]; has {
				glog.Infof("Setting stream's manifestID=%s playbackID=%s active status to false", id, playbackID)
				if mc.consulURL != nil {
					consulPlaybackID := mc.consulPrefix + playbackID
					go consul.DeleteKey(mc.consulURL, traefikKeyPathRouters+consulPlaybackID, true)
					// shouldn't exists with new scheme, but keeping here to clean up routes made with old scheme
					go consul.DeleteKey(mc.consulURL, traefikKeyPathServices+consulPlaybackID, true)
					if mc.baseStreamName != "" {
						go consul.DeleteKey(mc.consulURL, traefikKeyPathMiddlewares+consulPlaybackID, true)
					}
				}
				if mc.useEtcd {
					mc.deleteEtcdKeys(playbackID)
				}
				_, err := mc.lapi.SetActive(id, false)
				if err != nil {
					glog.Error(err)
				}
				delete(mc.pub2id, playbackID)
				metrics.StopStream(true)
			}
			mc.mu.Unlock()
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("yes"))
		return
	}
	if trigger != "RTMP_PUSH_REWRITE" {
		glog.Errorf("Got unsupported trigger: '%s'", trigger)
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("false"))
		return
	}
	if len(lines) < 2 {
		glog.Errorf("Expected 2 lines, got %d, request \n%s", len(lines), bs)
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("false"))
		return
	}
	// glog.V(model.INSANE).Infof("Parsed request (%d):\n%+v", len(lines), lines)
	pu, err := url.Parse(lines[0])
	responseURL := lines[0]
	if err != nil {
		glog.Errorf("Error parsing url=%s err=%v", lines[0], err)
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("false"))
		return
	}
	pp := strings.Split(pu.Path, "/")
	if len(pp) != 3 {
		glog.Errorf("URL wrongly formatted - should be in format rtmp://mist.host/live/streamKey")
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("false"))
		return
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
		return
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
		return
	}
	doLogRequestEnd = true

	if stream.PlaybackID != "" {
		mc.mu.Lock()
		defer mc.mu.Unlock()
		mc.pub2id[stream.PlaybackID] = stream.ID
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
			delete(mc.pub2id, stream.PlaybackID)
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte("false"))
			return
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
			return
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
		}
	}
	w.Write([]byte(responseURL))
	metrics.StartStream()
	glog.Infof("Responded with '%s'", responseURL)
	mc.startPushTargets(stream)
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
			glog.Errorf("mist-api-connector: success deleting keys for playbackID=%s rev=%d", playbackID, rev)
		} else {
			var curRev int64
			if len(resp.Responses) > 0 && len(resp.Responses[0].GetResponseRange().Kvs) > 0 {
				curRev = resp.Responses[0].GetResponseRange().Kvs[0].CreateRevision
			}
			glog.Errorf("mist-api-connector: unsuccessful deleting keys for playbackID=%s myRev=%d curRev=%d pathKey=%s",
				playbackID, rev, curRev, pathKey)
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

func (mc *mac) startPushTargets(stream *livepeer.CreateStreamResp) {
	if stream.PushTargets == nil {
		return
	}
	wildcardPlaybackID := mc.wildcardPlaybackID(stream)
	for _, target := range stream.PushTargets {
		go func(target livepeer.StreamPushTarget) {
			time.Sleep(30 * time.Second) // hack hack hack
			pushTarget, err := mc.lapi.GetPushTarget(target.ID)
			if err != nil {
				glog.Errorf("Error downloading PushTarget pushTargetId=%s stream=%s err=%v", target.ID, wildcardPlaybackID, err)
				return
			}
			// Find the actual parameters of the profile we're using
			var videoSelector string
			// Not actually the source. But the highest quality.
			if target.Profile == "source" {
				videoSelector = "maxbps"
			} else {
				var prof *livepeer.Profile
				for _, p := range stream.Profiles {
					if p.Name == target.Profile {
						prof = &p
						break
					}
				}
				if prof == nil {
					glog.Errorf("Error starting PushTarget pushTargetId=%s stream=%s err=couldn't find profile %s", target.ID, wildcardPlaybackID, target.Profile)
					return
				}
				videoSelector = fmt.Sprintf("~%dx%d", prof.Width, prof.Height)
			}
			// Inject ?video=~widthxheight to send the correct rendition
			selectorURL := fmt.Sprintf("%s?video=%s&audio=maxbps", pushTarget.URL, videoSelector)
			err = mc.mapi.StartPush(wildcardPlaybackID, selectorURL)
			if err != nil {
				glog.Errorf("Error starting PushTarget pushTargetId=%s stream=%s err=%v", target.ID, wildcardPlaybackID, err)
				return
			}
			glog.Infof("Started PushTarget stream=%s pushTargetId=%s", wildcardPlaybackID, target.ID)
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
	ids := make([]string, 0, len(mc.pub2id))
	for _, v := range mc.pub2id {
		ids = append(ids, v)
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
