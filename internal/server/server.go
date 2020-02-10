package server

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/golang/glog"
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
	mistMode  bool
}

// NewStreamerServer creates new StreamerServer
func NewStreamerServer(wowzaMode, mistMode bool) *StreamerServer {
	return &StreamerServer{
		// streamer:  testers.NewStreamer(wowzaMode),
		wowzaMode: wowzaMode,
		mistMode:  mistMode,
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
	w.Header().Set("Content-Type", "application/json")
	returnRawLatencies := false
	var baseManifestID string
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
		stats = ss.streamer.Stats(baseManifestID)
	}
	if !returnRawLatencies {
		stats.RawSourceLatencies = nil
		stats.RawTranscodedLatencies = nil
		stats.RawTranscodeLatenciesPerStream = nil
	}
	// glog.Infof("Lat avg %d p50 %d p95 %d p99 %d  avg %s p50 %s p95 %s p99 %s", stats.SourceLatencies.Avg, stats.SourceLatencies.P50, stats.SourceLatencies.P95,
	// 	stats.SourceLatencies.P99, stats.SourceLatencies.Avg, stats.SourceLatencies.P50, stats.SourceLatencies.P95, stats.SourceLatencies.P99)
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
			ss.streamer = testers.NewHTTPLoadTester()
		} else {
			ss.streamer = testers.NewStreamer(ss.wowzaMode, ss.mistMode, nil)
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
