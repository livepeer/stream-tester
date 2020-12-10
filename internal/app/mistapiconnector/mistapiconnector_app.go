package mistapiconnector

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/stream-tester/apis/consul"
	"github.com/livepeer/stream-tester/apis/livepeer"
	"github.com/livepeer/stream-tester/apis/mist"
	mistapi "github.com/livepeer/stream-tester/apis/mist"
	"github.com/livepeer/stream-tester/internal/metrics"
	"github.com/livepeer/stream-tester/internal/utils"
	"github.com/livepeer/stream-tester/model"
)

const streamPlaybackPrefix = "playback_"
const traefikRuleTemplate = "Host(`%s`) && PathPrefix(`/hls/%s/`)"
const traefikKeyPathRouters = `traefik/http/routers/`
const traefikKeyPathServices = `traefik/http/services/`
const traefikKeyPathMiddlewares = `traefik/http/middlewares/`
const audioAlways = "always"
const audioNever = "never"
const audioRecord = "record"
const audioEnabledStreamSuffix = "rec"

type (
	// IMac creates new Mist API Connector application
	IMac interface {
		SetupTriggers(ownURI string) error
		StartServer(bindAddr string) error
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

	mac struct {
		opts           *MacOptions
		mapi           *mist.API
		lapi           *livepeer.API
		balancerHost   string
		pub2id         map[string]string // public key to stream id
		mu             sync.Mutex
		mistHot        string
		checkBandwidth bool
		consulURL      *url.URL
		mistURL        string
		playbackDomain string
		sendAudio      string
		baseStreamName string
	}
)

// NewMac ...
func NewMac(mistHost string, mapi *mist.API, lapi *livepeer.API, balancerHost string, checkBandwidth bool, consul *url.URL, playbackDomain, mistURL,
	sendAudio, baseStreamName string) IMac {
	if balancerHost != "" && !strings.Contains(balancerHost, ":") {
		balancerHost = balancerHost + ":8042" // must set default port for Mist's Load Balancer
	}
	return &mac{
		mistHot:        mistHost,
		mapi:           mapi,
		lapi:           lapi,
		checkBandwidth: checkBandwidth,
		balancerHost:   balancerHost,
		pub2id:         make(map[string]string), // public key to stream id
		consulURL:      consul,
		mistURL:        mistURL,
		playbackDomain: playbackDomain,
		sendAudio:      sendAudio,
		baseStreamName: baseStreamName,
	}
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
					go consul.DeleteKey(mc.consulURL, traefikKeyPathRouters+playbackID, true)
					go consul.DeleteKey(mc.consulURL, traefikKeyPathServices+playbackID, true)
					if mc.baseStreamName != "" {
						go consul.DeleteKey(mc.consulURL, traefikKeyPathMiddlewares+playbackID, true)
					}
				}
				_, err := mc.lapi.SetActive(id, false)
				if err != nil {
					glog.Error(err)
				}
				delete(mc.pub2id, playbackID)
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
	w.Write([]byte(responseURL))
	if mc.consulURL != nil {
		// now create routing rule in the Consul for HLS playback
		go func() {
			var err error
			if mc.baseStreamName != "" {
				wildcardPlaybackID := mc.wildcardPlaybackID(stream)
				playbackID := stream.PlaybackID
				err = consul.PutKeys(
					mc.consulURL,
					traefikKeyPathRouters+playbackID+"/rule",
					fmt.Sprintf(traefikRuleTemplate, mc.playbackDomain, playbackID),
					traefikKeyPathRouters+playbackID+"/service",
					playbackID,
					traefikKeyPathRouters+playbackID+"/middlewares/0",
					playbackID+"-1",
					traefikKeyPathRouters+playbackID+"/middlewares/1",
					playbackID+"-2",

					traefikKeyPathMiddlewares+playbackID+"-1/stripprefix/prefixes/0",
					`/hls/`+playbackID,
					traefikKeyPathMiddlewares+playbackID+"-2/addprefix/prefix",
					`/hls/`+wildcardPlaybackID,

					// traefikKeyPathMiddlewares+playbackID+"/replacepathregex/regex",
					// fmt.Sprintf(`^/hls/%s\+(.*)`, mc.baseNameForStream(stream)),
					// traefikKeyPathMiddlewares+playbackID+"/replacepathregex/replacement",
					// `/hls/$1`,

					traefikKeyPathServices+playbackID+"/loadbalancer/servers/0/url",
					mc.mistURL,
					traefikKeyPathServices+playbackID+"/loadbalancer/passhostheader",
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
			}
		}()
	}
	glog.Infof("Responded with '%s'", responseURL)
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
	srv := &http.Server{
		Addr:    bindAddr,
		Handler: mux,
	}

	glog.Info("Web server listening on ", bindAddr)
	err := srv.ListenAndServe()
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
