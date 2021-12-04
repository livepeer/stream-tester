// Package livepeer API
package livepeer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/stream-tester/internal/metrics"
	"github.com/livepeer/stream-tester/internal/utils/uhttp"
	"github.com/livepeer/stream-tester/model"
)

// ErrNotExists returned if stream is not found
var ErrNotExists = errors.New("stream does not exists")

const httpTimeout = 4 * time.Second
const setActiveTimeout = 1500 * time.Millisecond

var defaultHTTPClient = &http.Client{
	// Transport: &http2.Transport{TLSClientConfig: tlsConfig},
	// Transport: &http2.Transport{AllowHTTP: true},
	Timeout: httpTimeout,
}

var hostName, _ = os.Hostname()

const (
	// ESHServer GCP? server
	ESHServer = "esh.livepeer.live"
	// ACServer Atlantic Crypto server
	// ACServer = "chi.livepeer-ac.live"
	ACServer = "livepeer.monster"

	livepeerAPIGeolocateURL = "http://livepeer.live/api/geolocate"
	ProdServer              = "livepeer.com"

	RecordingStatusWaiting = "waiting"
	RecordingStatusReady   = "ready"
)

type (
	// API object incapsulating Livepeer's hosted API
	API struct {
		choosenServer string
		accessToken   string
		presets       []string
		httpClient    *http.Client
		broadcasters  []string
	}

	geoResp struct {
		ChosenServer string `json:"chosenServer,omitempty"`
		Servers      []struct {
			Server   string `json:"server,omitempty"`
			Duration int    `json:"duration,omitempty"`
		} `json:"servers,omitempty"`
	}

	createStreamReq struct {
		Name    string   `json:"name,omitempty"`
		Presets []string `json:"presets,omitempty"`
		// one of
		// - P720p60fps16x9
		// - P720p30fps16x9
		// - P720p30fps4x3
		// - P576p30fps16x9
		// - P360p30fps16x9
		// - P360p30fps4x3
		// - P240p30fps16x9
		// - P240p30fps4x3
		// - P144p30fps16x9
		Profiles []Profile `json:"profiles"`
		Record   bool      `json:"record,omitempty"`
	}

	// Profile transcoding profile
	Profile struct {
		Name    string `json:"name,omitempty"`
		Width   int    `json:"width,omitempty"`
		Height  int    `json:"height,omitempty"`
		Bitrate int    `json:"bitrate"`
		Fps     int    `json:"fps"`
		FpsDen  int    `json:"fpsDen,omitempty"`
		Gop     string `json:"gop,omitempty"`
		Profile string `json:"profile,omitempty"` // enum: - H264Baseline - H264Main - H264High - H264ConstrainedHigh
	}

	MultistreamTargetRef struct {
		Profile   string `json:"profile,omitempty"`
		VideoOnly bool   `json:"videoOnly,omitempty"`
		ID        string `json:"id,omitempty"`
	}

	// CreateStreamResp returned by API
	CreateStreamResp struct {
		ID                         string    `json:"id,omitempty"`
		Name                       string    `json:"name,omitempty"`
		Presets                    []string  `json:"presets,omitempty"`
		Kind                       string    `json:"kind,omitempty"`
		UserID                     string    `json:"userId,omitempty"`
		StreamKey                  string    `json:"streamKey,omitempty"`
		PlaybackID                 string    `json:"playbackId,omitempty"`
		ParentID                   string    `json:"parentId,omitempty"`
		CreatedAt                  int64     `json:"createdAt,omitempty"`
		LastSeen                   int64     `json:"lastSeen,omitempty"`
		SourceSegments             int64     `json:"sourceSegments,omitempty"`
		TranscodedSegments         int64     `json:"transcodedSegments,omitempty"`
		SourceSegmentsDuration     float64   `json:"sourceSegmentsDuration,omitempty"`
		TranscodedSegmentsDuration float64   `json:"transcodedSegmentsDuration,omitempty"`
		Deleted                    bool      `json:"deleted,omitempty"`
		Record                     bool      `json:"record"`
		Profiles                   []Profile `json:"profiles,omitempty"`
		Errors                     []string  `json:"errors,omitempty"`
		Multistream                struct {
			Targets []MultistreamTargetRef `json:"targets,omitempty"`
		} `json:"multistream"`
	}

	// UserSession user's sessions
	UserSession struct {
		CreateStreamResp
		RecordingStatus string `json:"recordingStatus,omitempty"` // ready, waiting
		RecordingURL    string `json:"recordingUrl,omitempty"`
		Mp4Url          string `json:"mp4Url,omitempty"`
	}

	MultistreamTarget struct {
		ID        string `json:"id,omitempty"`
		URL       string `json:"url,omitempty"`
		Name      string `json:"name,omitempty"`
		UserId    string `json:"userId,omitempty"`
		Disabled  bool   `json:"disabled,omitempty"`
		CreatedAt int64  `json:"createdAt,omitempty"`
	}

	// // Profile ...
	// Profile struct {
	// 	Fps     int    `json:"fps"`
	// 	Name    string `json:"name,omitempty"`
	// 	Width   int    `json:"width,omitempty"`
	// 	Height  int    `json:"height,omitempty"`
	// 	Bitrate int    `json:"bitrate,omitempty"`
	// }

	addressResp struct {
		Address string `json:"address"`
	}

	setActiveReq struct {
		Active    bool   `json:"active"`
		HostName  string `json:"hostName"`
		StartedAt int64  `json:"startedAt"`
	}

	deactivateManyReq struct {
		IDS []string `json:"ids"`
	}

	deactivateManyResp struct {
		RowCount int `json:"rowCount"`
	}
)

// NewLivepeer creates new Livepeer API object
func NewLivepeer(livepeerToken, serverOverride string, presets []string) *API {
	return &API{
		choosenServer: addScheme(serverOverride),
		accessToken:   livepeerToken,
		presets:       presets,
		httpClient:    defaultHTTPClient,
	}
}

// NewLivepeer2 creates new Livepeer API object
func NewLivepeer2(livepeerToken, serverOverride string, presets []string, timeout time.Duration) *API {
	httpClient := defaultHTTPClient
	if timeout != 0 {
		httpClient = &http.Client{
			Timeout: timeout,
		}

	}
	return &API{
		choosenServer: addScheme(serverOverride),
		accessToken:   livepeerToken,
		presets:       presets,
		httpClient:    httpClient,
	}
}

func addScheme(uri string) string {
	if uri == "" {
		return uri
	}
	luri := strings.ToLower(uri)
	if strings.HasPrefix(luri, "http://") || strings.HasPrefix(luri, "https://") {
		return uri
	}
	if strings.Contains(luri, ".local") || strings.HasPrefix(luri, "localhost") {
		return "http://" + uri
	}
	return "https://" + uri
}

// GetServer returns choosen server
func (lapi *API) GetServer() string {
	return lapi.choosenServer
}

// Init calles geolocation API endpoint to find closes server
// do nothing if `serverOverride` was not empty in the `NewLivepeer` call
func (lapi *API) Init() {
	if lapi.choosenServer != "" {
		return
	}

	resp, err := lapi.httpClient.Do(uhttp.GetRequest(livepeerAPIGeolocateURL))
	if err != nil {
		glog.Fatalf("Error geolocating Livepeer API server (%s) error: %v", livepeerAPIGeolocateURL, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := ioutil.ReadAll(resp.Body)
		glog.Fatalf("Status error contacting Livepeer API server (%s) status %d body: %s", livepeerAPIGeolocateURL, resp.StatusCode, string(b))
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.Fatalf("Error geolocating Livepeer API server (%s) error: %v", livepeerAPIGeolocateURL, err)
	}
	glog.Info(string(b))
	geo := &geoResp{}
	err = json.Unmarshal(b, geo)
	if err != nil {
		panic(err)
	}
	glog.Infof("chosen server: %s, servers num: %d", geo.ChosenServer, len(geo.Servers))
	lapi.choosenServer = addScheme(geo.ChosenServer)
}

// Broadcasters returns list of hostnames of broadcasters to use
func (lapi *API) Broadcasters() ([]string, error) {
	u := fmt.Sprintf("%s/api/broadcaster", lapi.choosenServer)
	resp, err := lapi.httpClient.Do(uhttp.GetRequest(u))
	if err != nil {
		glog.Errorf("Error getting broadcasters from Livepeer API server (%s) error: %v", u, err)
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		glog.Fatalf("Status error contacting Livepeer API server (%s) status %d body: %s", livepeerAPIGeolocateURL, resp.StatusCode, string(b))
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.Fatalf("Error geolocating Livepeer API server (%s) error: %v", livepeerAPIGeolocateURL, err)
	}
	glog.Info(string(b))
	broadcasters := []addressResp{}
	err = json.Unmarshal(b, &broadcasters)
	if err != nil {
		return nil, err
	}
	bs := make([]string, 0, len(broadcasters))
	for _, a := range broadcasters {
		bs = append(bs, a.Address)
	}
	return bs, nil
}

// Ingest object
type Ingest struct {
	Base     string `json:"base,omitempty"`
	Playback string `json:"playback,omitempty"`
	Ingest   string `json:"ingest,omitempty"`
}

// Ingest returns ingest object
func (lapi *API) Ingest(all bool) ([]Ingest, error) {
	u := fmt.Sprintf("%s/api/ingest", lapi.choosenServer)
	if all {
		u += "?first=false"
	}
	resp, err := lapi.httpClient.Do(uhttp.GetRequest(u))
	if err != nil {
		glog.Errorf("Error getting ingests from Livepeer API server (%s) error: %v", u, err)
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := ioutil.ReadAll(resp.Body)
		glog.Fatalf("Status error contacting Livepeer API server (%s) status %d body: %s", lapi.choosenServer, resp.StatusCode, string(b))
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.Fatalf("Error reading from Livepeer API server (%s) error: %v", lapi.choosenServer, err)
	}
	glog.Info(string(b))
	ingests := []Ingest{}
	err = json.Unmarshal(b, &ingests)
	if err != nil {
		return nil, err
	}
	return ingests, nil
}

var StandardProfiles = []Profile{
	{
		Name:    "240p0",
		Fps:     0,
		Bitrate: 250000,
		Width:   426,
		Height:  240,
		Gop:     "2.0",
	},
	{
		Name:    "360p0",
		Fps:     0,
		Bitrate: 800000,
		Width:   640,
		Height:  360,
		Gop:     "2.0",
	},
	{
		Name:    "480p0",
		Fps:     0,
		Bitrate: 1600000,
		Width:   854,
		Height:  480,
		Gop:     "2.0",
	},
	{
		Name:    "720p0",
		Fps:     0,
		Bitrate: 3000000,
		Width:   1280,
		Height:  720,
		Gop:     "2.0",
	},
}

// CreateStream creates stream with specified name and profiles
func (lapi *API) CreateStream(name string, presets ...string) (string, error) {
	csr, err := lapi.CreateStreamEx(name, false, presets)
	if err != nil {
		return "", err
	}
	return csr.ID, err
}

// DeleteStream deletes stream
func (lapi *API) DeleteStream(id string) error {
	glog.V(model.DEBUG).Infof("Deleting Livepeer stream '%s' ", id)
	u := fmt.Sprintf("%s/api/stream/%s", lapi.choosenServer, id)
	req, err := uhttp.NewRequest("DELETE", u, nil)
	if err != nil {
		return err
	}
	req.Header.Add("Authorization", "Bearer "+lapi.accessToken)
	resp, err := lapi.httpClient.Do(req)
	if err != nil {
		glog.Errorf("Error deleting Livepeer stream %v", err)
		return err
	}
	defer resp.Body.Close()
	_, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.Errorf("Error deleting Livepeer stream (body) %v", err)
		return err
	}
	if resp.StatusCode != 204 {
		return fmt.Errorf("error deleting stream %s: status is %s", id, resp.Status)
	}
	return nil
}

// CreateStreamEx creates stream with specified name and profiles
func (lapi *API) CreateStreamEx(name string, record bool, presets []string, profiles ...Profile) (*CreateStreamResp, error) {
	return lapi.CreateStreamEx2(name, record, "", presets, profiles...)
}

// CreateStreamEx2R creates stream with specified name and profiles
func (lapi *API) CreateStreamEx2R(name string, record bool, parentID string, presets []string, profiles ...Profile) (*CreateStreamResp, error) {
	var apiTry int
	for {
		stream, err := lapi.CreateStreamEx2(name, record, parentID, presets, profiles...)
		if err != nil {
			if Timedout(err) && apiTry < 3 {
				apiTry++
				continue
			}
			glog.Errorf("Error creating stream using Livepeer API: %v", err)
			return nil, err
		}
		return stream, err
	}
}

// CreateStreamEx2 creates stream with specified name and profiles
func (lapi *API) CreateStreamEx2(name string, record bool, parentID string, presets []string, profiles ...Profile) (*CreateStreamResp, error) {
	// presets := profiles
	// if len(presets) == 0 {
	// 	presets = lapi.presets
	// }
	glog.Infof("Creating Livepeer stream '%s' with presets '%v' and profiles %+v", name, presets, profiles)
	reqs := &createStreamReq{
		Name:    name,
		Presets: presets,
		Record:  record,
	}
	if len(presets) == 0 {
		reqs.Profiles = StandardProfiles
	}
	if len(profiles) > 0 {
		reqs.Profiles = profiles
	} else {
		reqs.Profiles = make([]Profile, 0)
	}
	b, err := json.Marshal(reqs)
	if err != nil {
		glog.V(model.SHORT).Infof("Error marshalling create stream request %v", err)
		return nil, err
	}
	glog.Infof("Sending: %s", b)
	u := fmt.Sprintf("%s/api/stream", lapi.choosenServer)
	if parentID != "" {
		u = fmt.Sprintf("%s/api/stream/%s/stream", lapi.choosenServer, parentID)
	}
	req, err := uhttp.NewRequest("POST", u, bytes.NewBuffer(b))
	if err != nil {
		return nil, err
	}
	req.Header.Add("Authorization", "Bearer "+lapi.accessToken)
	req.Header.Add("Content-Type", "application/json")
	resp, err := lapi.httpClient.Do(req)
	if err != nil {
		glog.Errorf("Error creating Livepeer stream %v", err)
		return nil, err
	}
	defer resp.Body.Close()
	b, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.Errorf("Error creating Livepeer stream (body) %v", err)
		return nil, err
	}
	glog.Info(string(b))
	r := &CreateStreamResp{}
	err = json.Unmarshal(b, r)
	if err != nil {
		return nil, err
	}
	if len(r.Errors) > 0 {
		return nil, fmt.Errorf("Error creating stream: %+v", r.Errors)
	}
	glog.Infof("Stream %s created with id %s", name, r.ID)
	return r, nil
}

// DefaultPresets returns default presets
func (lapi *API) DefaultPresets() []string {
	return lapi.presets
}

// GetStreamByKey gets stream by streamKey
func (lapi *API) GetStreamByKey(key string) (*CreateStreamResp, error) {
	if key == "" {
		return nil, errors.New("empty key")
	}
	u := fmt.Sprintf("%s/api/stream/key/%s?main=true", lapi.choosenServer, key)
	return lapi.getStream(u, "get_by_key")
}

// GetStreamByPlaybackID gets stream by playbackID
func (lapi *API) GetStreamByPlaybackID(playbackID string) (*CreateStreamResp, error) {
	if playbackID == "" {
		return nil, errors.New("empty playbackID")
	}
	u := fmt.Sprintf("%s/api/stream/playback/%s", lapi.choosenServer, playbackID)
	return lapi.getStream(u, "get_by_playbackid")
}

// GetStream gets stream by id
func (lapi *API) GetStream(id string) (*CreateStreamResp, error) {
	if id == "" {
		return nil, errors.New("empty id")
	}
	u := fmt.Sprintf("%s/api/stream/%s", lapi.choosenServer, id)
	return lapi.getStream(u, "get_by_id")
}

// GetSessionsR gets user's sessions for the stream by id
func (lapi *API) GetSessionsR(id string, forceUrl bool) ([]UserSession, error) {
	var apiTry int
	for {
		sessions, err := lapi.GetSessions(id, forceUrl)
		if err != nil {
			if Timedout(err) && apiTry < 3 {
				apiTry++
				continue
			}
		}
		return sessions, err
	}
}

// GetSessionsNewR gets user's sessions for the stream by id
func (lapi *API) GetSessionsNewR(id string, forceUrl bool) ([]UserSession, error) {
	var apiTry int
	for {
		sessions, err := lapi.GetSessionsNew(id, forceUrl)
		if err != nil {
			if Timedout(err) && apiTry < 3 {
				apiTry++
				continue
			}
		}
		return sessions, err
	}
}

func (lapi *API) GetSessionsNew(id string, forceUrl bool) ([]UserSession, error) {
	if id == "" {
		return nil, errors.New("empty id")
	}
	u := fmt.Sprintf("%s/api/session?parentId=%s", lapi.choosenServer, id)
	if forceUrl {
		u += "&forceUrl=1"
	}
	start := time.Now()
	req := uhttp.GetRequest(u)
	req.Header.Add("Authorization", "Bearer "+lapi.accessToken)
	resp, err := lapi.httpClient.Do(req)
	if err != nil {
		glog.Errorf("Error getting sessions for stream by id from Livepeer API server (%s) error: %v", u, err)
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := ioutil.ReadAll(resp.Body)
		glog.Errorf("Status error getting sessions for stream by id Livepeer API server (%s) status %d body: %s", u, resp.StatusCode, string(b))
		if resp.StatusCode == http.StatusNotFound {
			return nil, ErrNotExists
		}
		err := errors.New(http.StatusText(resp.StatusCode))
		return nil, err
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.Errorf("Error getting sessions for stream by id Livepeer API server (%s) error: %v", u, err)
		return nil, err
	}
	took := time.Since(start)
	glog.V(model.DEBUG).Infof("sessions request for id=%s took=%s", id, took)
	bs := string(b)
	glog.Info(bs)
	glog.V(model.VERBOSE).Info(bs)
	if bs == "null" || bs == "" {
		// API return null if stream does not exists
		return nil, ErrNotExists
	}
	r := []UserSession{}
	err = json.Unmarshal(b, &r)
	if err != nil {
		return nil, err
	}
	return r, nil
}

// GetSessions gets user's sessions for the stream by id
func (lapi *API) GetSessions(id string, forceUrl bool) ([]UserSession, error) {
	if id == "" {
		return nil, errors.New("empty id")
	}
	u := fmt.Sprintf("%s/api/stream/%s/sessions", lapi.choosenServer, id)
	if forceUrl {
		u += "?forceUrl=1"
	}
	start := time.Now()
	req := uhttp.GetRequest(u)
	req.Header.Add("Authorization", "Bearer "+lapi.accessToken)
	resp, err := lapi.httpClient.Do(req)
	if err != nil {
		glog.Errorf("Error getting sessions for stream by id from Livepeer API server (%s) error: %v", u, err)
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := ioutil.ReadAll(resp.Body)
		glog.Errorf("Status error getting sessions for stream by id Livepeer API server (%s) status %d body: %s", u, resp.StatusCode, string(b))
		if resp.StatusCode == http.StatusNotFound {
			return nil, ErrNotExists
		}
		err := errors.New(http.StatusText(resp.StatusCode))
		return nil, err
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.Errorf("Error getting sessions for stream by id Livepeer API server (%s) error: %v", u, err)
		return nil, err
	}
	took := time.Since(start)
	glog.V(model.DEBUG).Infof("sessions request for id=%s took=%s", id, took)
	bs := string(b)
	glog.V(model.VERBOSE).Info(bs)
	if bs == "null" || bs == "" {
		// API return null if stream does not exists
		return nil, ErrNotExists
	}
	r := []UserSession{}
	err = json.Unmarshal(b, &r)
	if err != nil {
		return nil, err
	}
	return r, nil
}

// SetActiveR sets stream active with retries
func (lapi *API) SetActiveR(id string, active bool, startedAt time.Time) (bool, error) {
	apiTry := 1
	for {
		ok, err := lapi.SetActive(id, active, startedAt)
		if err != nil {
			if Timedout(err) && apiTry < 3 {
				apiTry++
				continue
			}
			glog.Errorf("Fatal error calling API /setactive id=%s active=%s err=%v", id, active, err)
		}
		return ok, err
	}
}

// SetActive set isActive
func (lapi *API) SetActive(id string, active bool, startedAt time.Time) (bool, error) {
	if id == "" {
		return true, errors.New("empty id")
	}
	start := time.Now()
	u := fmt.Sprintf("%s/api/stream/%s/setactive", lapi.choosenServer, id)
	ar := setActiveReq{
		Active:   active,
		HostName: hostName,
	}
	if !startedAt.IsZero() {
		ar.StartedAt = startedAt.UnixNano() / int64(time.Millisecond)
	}
	b, _ := json.Marshal(&ar)
	req, err := uhttp.NewRequest("PUT", u, bytes.NewBuffer(b))
	if err != nil {
		metrics.APIRequest("set_active", 0, err)
		return true, err
	}
	ctx, cancel := context.WithTimeout(req.Context(), setActiveTimeout)
	defer cancel()
	req = req.WithContext(ctx)

	req.Header.Add("Authorization", "Bearer "+lapi.accessToken)
	req.Header.Add("Content-Type", "application/json")
	resp, err := lapi.httpClient.Do(req)
	if err != nil {
		glog.Errorf("id=%s/setactive Error set active %v", id, err)
		metrics.APIRequest("set_active", 0, err)
		return true, err
	}
	defer resp.Body.Close()
	b, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.Errorf("id=%s/setactive Error set active (body) %v", err)
		metrics.APIRequest("set_active", 0, err)
		return true, err
	}
	took := time.Since(start)
	metrics.APIRequest("set_active", took, nil)
	glog.Infof("%s/setactive took=%s response status code %d status %s resp %+v body=%s",
		id, took, resp.StatusCode, resp.Status, resp, string(b))
	return resp.StatusCode >= 200 && resp.StatusCode < 300, nil
}

// DeactivateMany sets many streams isActive field to false
func (lapi *API) DeactivateMany(ids []string) (int, error) {
	if len(ids) == 0 {
		return 0, errors.New("empty ids")
	}
	start := time.Now()
	u := fmt.Sprintf("%s/api/stream/deactivate-many", lapi.choosenServer)
	dmreq := deactivateManyReq{
		IDS: ids,
	}
	b, _ := json.Marshal(&dmreq)
	req, err := uhttp.NewRequest("PATCH", u, bytes.NewBuffer(b))
	if err != nil {
		metrics.APIRequest("deactivate-many", 0, err)
		return 0, err
	}
	req.Header.Add("Authorization", "Bearer "+lapi.accessToken)
	req.Header.Add("Content-Type", "application/json")
	resp, err := lapi.httpClient.Do(req)
	if err != nil {
		glog.Errorf("/deactivate-many err=%v", err)
		metrics.APIRequest("set_active", 0, err)
		return 0, err
	}
	defer resp.Body.Close()
	b, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.Errorf("deactivate-many body err=%v", err)
		metrics.APIRequest("deactivate-many", 0, err)
		return 0, err
	}
	took := time.Since(start)
	metrics.APIRequest("deactivate-many", took, nil)
	glog.Infof("deactivate-many took=%s response status code %d status %s resp %+v body=%s",
		took, resp.StatusCode, resp.Status, resp, string(b))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return 0, fmt.Errorf("invalid status code: %d", resp.StatusCode)
	}

	mr := &deactivateManyResp{}
	err = json.Unmarshal(b, mr)
	if err != nil {
		return 0, err
	}

	return mr.RowCount, nil
}

func (lapi *API) getStream(u, rType string) (*CreateStreamResp, error) {
	start := time.Now()
	req := uhttp.GetRequest(u)
	req.Header.Add("Authorization", "Bearer "+lapi.accessToken)
	resp, err := lapi.httpClient.Do(req)
	if err != nil {
		glog.Errorf("Error getting stream by id from Livepeer API server (%s) error: %v", u, err)
		metrics.APIRequest(rType, 0, err)
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := ioutil.ReadAll(resp.Body)
		glog.Errorf("Status error getting stream by id Livepeer API server (%s) status %d body: %s", u, resp.StatusCode, string(b))
		if resp.StatusCode == http.StatusNotFound {
			metrics.APIRequest(rType, 0, ErrNotExists)
			return nil, ErrNotExists
		}
		err := errors.New(http.StatusText(resp.StatusCode))
		metrics.APIRequest(rType, 0, err)
		return nil, err
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.Errorf("Error getting stream by id Livepeer API server (%s) error: %v", u, err)
		metrics.APIRequest(rType, 0, err)
		return nil, err
	}
	took := time.Since(start)
	metrics.APIRequest(rType, took, nil)
	bs := string(b)
	glog.V(model.VERBOSE).Info(bs)
	if bs == "null" {
		// API return null if stream does not exists
		return nil, ErrNotExists
	}
	r := &CreateStreamResp{}
	err = json.Unmarshal(b, r)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (lapi *API) GetMultistreamTarget(id string) (*MultistreamTarget, error) {
	rType := "get_multistream_target"
	start := time.Now()
	u := fmt.Sprintf("%s/api/multistream/target/%s", lapi.choosenServer, id)
	req := uhttp.GetRequest(u)
	req.Header.Add("Authorization", "Bearer "+lapi.accessToken)
	resp, err := lapi.httpClient.Do(req)
	if err != nil {
		glog.Errorf("Error getting MultistreamTarget by id from Livepeer API server (%s) error: %v", u, err)
		metrics.APIRequest(rType, 0, err)
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := ioutil.ReadAll(resp.Body)
		glog.Errorf("Status error getting MultistreamTarget by id Livepeer API server (%s) status %d body: %s", u, resp.StatusCode, string(b))
		if resp.StatusCode == http.StatusNotFound {
			metrics.APIRequest(rType, 0, ErrNotExists)
			return nil, ErrNotExists
		}
		err := errors.New(http.StatusText(resp.StatusCode))
		metrics.APIRequest(rType, 0, err)
		return nil, err
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.Errorf("Error getting MultistreamTarget by id Livepeer API server (%s) error: %v", u, err)
		metrics.APIRequest(rType, 0, err)
		return nil, err
	}
	took := time.Since(start)
	metrics.APIRequest(rType, took, nil)
	bs := string(b)
	glog.V(model.VERBOSE).Info(bs)
	if bs == "null" {
		// API return null if stream does not exists
		return nil, ErrNotExists
	}
	r := &MultistreamTarget{}
	err = json.Unmarshal(b, r)
	if err != nil {
		return nil, err
	}
	return r, nil
}

// GetMultistreamTargetR gets multistream target with retries
func (lapi *API) GetMultistreamTargetR(id string) (*MultistreamTarget, error) {
	var apiTry int
	for {
		target, err := lapi.GetMultistreamTarget(id)
		if err != nil {
			if Timedout(err) && apiTry < 3 {
				apiTry++
				continue
			}
		}
		return target, err
	}
}

func (lapi *API) PushSegment(sid string, seqNo int, dur time.Duration, segData []byte) ([][]byte, error) {
	var err error
	if len(lapi.broadcasters) == 0 {
		lapi.broadcasters, err = lapi.Broadcasters()
		if err != nil {
			return nil, err
		}
		if len(lapi.broadcasters) == 0 {
			return nil, fmt.Errorf("no broadcasters available")
		}
	}
	urlToUp := fmt.Sprintf("%s/live/%s/%d.ts", lapi.broadcasters[0], sid, seqNo)
	var body io.Reader
	body = bytes.NewReader(segData)
	req, err := uhttp.NewRequest("POST", urlToUp, body)
	if err != nil {
		panic(err)
	}
	req.Header.Set("Accept", "multipart/mixed")
	req.Header.Set("Content-Duration", strconv.FormatInt(dur.Milliseconds(), 10))
	postStarted := time.Now()
	resp, err := lapi.httpClient.Do(req)
	postTook := time.Since(postStarted)
	var timedout bool
	var status string
	if err != nil {
		uerr := err.(*url.Error)
		timedout = uerr.Timeout()
	}
	if resp != nil {
		status = resp.Status
	}
	glog.V(model.DEBUG).Infof("Post segment manifest=%s seqNo=%d dur=%s took=%s timed_out=%v status='%v' err=%v",
		sid, seqNo, dur, postTook, timedout, status, err)
	if err != nil {
		return nil, err
	}
	glog.V(model.VERBOSE).Infof("Got transcoded manifest=%s seqNo=%d resp status=%s reading body started", sid, seqNo, resp.Status)
	if resp.StatusCode != http.StatusOK {
		b, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		glog.V(model.DEBUG).Infof("Got manifest=%s seqNo=%d resp status=%s error in body $%s", sid, seqNo, resp.Status, string(b))
		return nil, fmt.Errorf("transcode error %s: %s", resp.Status, b)
	}
	started := time.Now()
	mediaType, params, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil {
		glog.Error("Error getting mime type ", err, sid)
		panic(err)
		// return
	}
	glog.V(model.VERBOSE).Infof("mediaType=%s params=%+v", mediaType, params)
	if glog.V(model.VVERBOSE) {
		for k, v := range resp.Header {
			glog.Infof("Header '%s': '%s'", k, v)
		}
	}
	var segments [][]byte
	var urls []string
	if mediaType == "multipart/mixed" {
		mr := multipart.NewReader(resp.Body, params["boundary"])
		for {
			p, merr := mr.NextPart()
			if merr == io.EOF {
				break
			}
			if merr != nil {
				glog.Error("Could not process multipart part ", merr, sid)
				err = merr
				break
			}
			mediaType, _, err := mime.ParseMediaType(p.Header.Get("Content-Type"))
			if err != nil {
				glog.Error("Error getting mime type ", err, sid)
				for k, v := range p.Header {
					glog.Infof("Header '%s': '%s'", k, v)
				}
			}
			body, merr := ioutil.ReadAll(p)
			if merr != nil {
				glog.Errorf("error reading body manifest=%s seqNo=%d err=%v", sid, seqNo, merr)
				err = merr
				break
			}
			if mediaType == "application/vnd+livepeer.uri" {
				urls = append(urls, string(body))
			} else {
				var v glog.Level = model.DEBUG
				if len(body) < 5 {
					v = 0
				}
				glog.V(v).Infof("Read back segment for manifest=%s seqNo=%d profile=%d len=%d bytes", sid, seqNo, len(segments), len(body))
				segments = append(segments, body)
			}
		}
	}
	took := time.Since(started)
	glog.V(model.VERBOSE).Infof("Reading body back for manifest=%s seqNo=%d took=%s profiles=%d", sid, seqNo, took, len(segments))
	// glog.Infof("Body: %s", string(tbody))

	if err != nil {
		httpErr := fmt.Errorf(`error reading http request body for manifest=%s seqNo=%d err=%w`, sid, seqNo, err)
		glog.Error(httpErr)
		return nil, err
	}
	return segments, nil
}

func Timedout(e error) bool {
	t, ok := e.(interface {
		Timeout() bool
	})
	return ok && t.Timeout() || (e != nil && strings.Contains(e.Error(), "Client.Timeout"))
}
