// Package broadcaster Livepeer's broadcater API
package broadcaster

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/m3u8"
	"github.com/livepeer/stream-tester/internal/utils/uhttp"
)

const httpTimeout = 2 * time.Second

var httpClient = &http.Client{
	Timeout: httpTimeout,
}

type (
	// RemoteTranscoderInfo ...
	RemoteTranscoderInfo struct {
		Address  string
		Capacity int
	}

	// StatusResp response of /status
	StatusResp struct {
		Manifests                   map[string]*m3u8.MasterPlaylist
		OrchestratorPool            []string
		Version                     string
		GolangRuntimeVersion        string
		GOArch                      string
		GOOS                        string
		RegisteredTranscodersNumber int
		RegisteredTranscoders       []RemoteTranscoderInfo
		LocalTranscoding            bool // Indicates orchestrator that is also transcoder
	}
)

// Status make call to `/status` endpoint of Livepeer's node
func Status(uri string) (*StatusResp, error) {
	resp, err := httpClient.Do(uhttp.GetRequest(uri))
	if err != nil {
		glog.Fatalf("Error getting status  (%s) error: %v", uri, err)
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		glog.Fatalf("Status error contacting Broadcaster (%s) status %d body: %s", uri, resp.StatusCode, string(b))
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.Fatalf("Error getting status (%s) error: %v", uri, err)
	}
	glog.Info(string(b))
	st := &StatusResp{}
	err = json.Unmarshal(b, st)
	if err != nil {
		panic(err)
	}
	return st, nil
}

// NewerManifests returns manifests that present in this status but not in the old one
func (st *StatusResp) NewerManifests(oldStatus *StatusResp) []string {
	res := make([]string, 0)
	for k := range st.Manifests {
		if _, has := oldStatus.Manifests[k]; !has {
			res = append(res, k)
		}
	}
	return res
}
