package server

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	// pprof adds handlers to default mux via `init()`
	"net/http/pprof"

	"github.com/golang/glog"
	"github.com/livepeer/stream-tester/apis/livepeer"
	mistapi "github.com/livepeer/stream-tester/apis/mist"
	"github.com/livepeer/stream-tester/internal/testers"
	"github.com/livepeer/stream-tester/model"
)

// StreamerServer implements web server, to be used in test harness
// provides REST endpoints to start streaming, stop and get statistics
type StreamerServer struct {
	// HTTPMux  *http.ServeMux
	streamer  model.Streamer
	lock      sync.RWMutex
	wowzaMode bool
	lapiToken string
	mistCreds []string
}

// NewStreamerServer creates new StreamerServer
func NewStreamerServer(wowzaMode bool, lapiToken, mistCreds string) *StreamerServer {
	var mcreds []string
	if mistCreds != "" {
		mcreds = strings.Split(mistCreds, ":")
		if len(mcreds) != 2 {
			glog.Fatal("Mist server's credentials should be in form 'login:password'")
		}
	}
	return &StreamerServer{
		// streamer:  testers.NewStreamer(wowzaMode),
		wowzaMode: wowzaMode,
		lapiToken: lapiToken,
		mistCreds: mcreds,
	}
}

// StartWebServer starts web server
// blocks until exit
func (ss *StreamerServer) StartWebServer(bindAddr string) {
	mux := ss.webServerHandlers(bindAddr)
	srv := &http.Server{
		Addr:    bindAddr,
		Handler: mux,
	}

	glog.Info("Web server listening on ", bindAddr)
	srv.ListenAndServe()
}

func (ss *StreamerServer) webServerHandlers(bindAddr string) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	mux.HandleFunc("/start_streams", ss.handleStartStreams)
	mux.HandleFunc("/stats", ss.handleStats)
	mux.HandleFunc("/stop", ss.handleStop)
	return mux
}

// Stop currently running streams
func (ss *StreamerServer) handleStop(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	glog.Info("Got stop request.")
	if ss.streamer != nil {
		ss.streamer.Stop()
	}
	w.WriteHeader(http.StatusOK)
}

// Set the broadcast config for creating onchain jobs.
func (ss *StreamerServer) handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	glog.Infof("requested %s", r.URL)
	w.Header().Set("Content-Type", "application/json")
	returnRawLatencies := false
	var baseManifestID string
	var err error
	if _, ok := r.URL.Query()["latencies"]; ok {
		returnRawLatencies = true
	}
	if bmids, ok := r.URL.Query()["base_manifest_id"]; ok {
		if len(bmids) > 0 {
			baseManifestID = bmids[0]
		}
	}
	stats := &model.Stats{}
	if ss.streamer != nil {
		stats, err = ss.streamer.Stats(baseManifestID)
	} else {
		w.WriteHeader(http.StatusNotFound)
		emsg := "No streamer exists"
		glog.Errorln(emsg)
		w.Write([]byte(emsg))
		return
	}
	if !returnRawLatencies {
		stats.RawSourceLatencies = nil
		stats.RawTranscodedLatencies = nil
		stats.RawTranscodeLatenciesPerStream = nil
	}
	glog.Infof("baseManifestID=%s", baseManifestID)
	if baseManifestID != "" && err == model.ErroNotFound {
		w.WriteHeader(http.StatusNotFound)
		emsg := fmt.Sprintf("not found stats for baseManifestID=%s", baseManifestID)
		glog.Errorln(emsg)
		w.Write([]byte(emsg))
		return
	}
	// glog.Infof("Lat avg %d p50 %d p95 %d p99 %d  avg %s p50 %s p95 %s p99 %s", stats.SourceLatencies.Avg, stats.SourceLatencies.P50, stats.SourceLatencies.P95,
	// 	stats.SourceLatencies.P99, stats.SourceLatencies.Avg, stats.SourceLatencies.P50, stats.SourceLatencies.P95, stats.SourceLatencies.P99)
	glog.Infof("stats: %+v", stats)
	b, err := json.Marshal(stats)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}
	w.Write(b)
}

// Set the broadcast config for creating onchain jobs.
func (ss *StreamerServer) handleStartStreams(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	glog.Infof("Got request: '%s'", string(b))
	ssr := &model.StartStreamsReq{}
	err = json.Unmarshal(b, ssr)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}
	glog.Infof("Start streams request %+v", *ssr)
	if ssr.Host == "" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Should specify 'host' field"))
		return
	}
	if ssr.MHost == "" {
		ssr.MHost = ssr.Host
	}
	if ssr.Repeat <= 0 {
		ssr.Repeat = 1
	}
	if ssr.Simultaneous <= 0 {
		ssr.Simultaneous = 1
	}
	if ssr.FileName == "" {
		ssr.FileName = "BigBuckBunny.mp4"
	}
	if ssr.RTMP == 0 {
		ssr.RTMP = 1935
	}
	if ssr.Media == 0 {
		ssr.Media = 8935
	}
	if ssr.ProfilesNum != 0 {
		model.ProfilesNum = ssr.ProfilesNum
	}
	if _, err := os.Stat(ssr.FileName); os.IsNotExist(err) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`File ` + ssr.FileName + ` does not exists`))
		return
	}
	glog.Infof("Get request: %+v", ssr)
	if !ssr.DoNotClearStats || ss.streamer == nil {
		if ssr.HTTPIngest {
			var lapi *livepeer.API
			if ssr.Lapi {
				presetsParts := strings.Split(ssr.Presets, ",")
				model.ProfilesNum = len(presetsParts)
				lapi = livepeer.NewLivepeer(ss.lapiToken, livepeer.ACServer, presetsParts) // hardcode AC server for now
				lapi.Init()
			}
			ss.streamer = testers.NewHTTPLoadTester(lapi, 0)
		} else {
			var mapi *mistapi.API
			if ssr.Mist {
				mapi = mistapi.NewMist(ssr.Host, ss.mistCreds[0], ss.mistCreds[1], ss.lapiToken)
				mapi.Login()
			}
			ss.streamer = testers.NewStreamer(ss.wowzaMode, ssr.Mist, mapi, nil)
		}
	}
	var streamDuration time.Duration
	if ssr.Time != "" {
		if streamDuration, err = ParseStreamDurationArgument(ssr.Time); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(err.Error()))
			return
		}
	}

	baseManifestID, err := ss.streamer.StartStreams(ssr.FileName, ssr.Host, strconv.Itoa(int(ssr.RTMP)), ssr.MHost, strconv.Itoa(int(ssr.Media)), ssr.Simultaneous,
		ssr.Repeat, streamDuration, true, ssr.MeasureLatency, true, 3, 5*time.Second, 0)

	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	res, err := json.Marshal(
		&model.StartStreamsRes{
			Success:        true,
			BaseManifestID: baseManifestID,
		},
	)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}
	w.Write(res)

}
