// Package mist API
package mist

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/stream-tester/internal/utils/uhttp"
)

const httpTimeout = 2 * time.Second

var httpClient = &http.Client{
	// Transport: &http2.Transport{TLSClientConfig: tlsConfig},
	// Transport: &http2.Transport{AllowHTTP: true},
	// Transport: &http2.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, AllowHTTP: true},
	Timeout: httpTimeout,
}

type (

	// API object incapsulating Mist's server API
	API struct {
		host              string
		apiURL            string
		login             string
		password          string
		livepeerToken     string
		challenge         string // value of challenge field sent by Mist
		challengeRepsonse string
	}

	authorize struct {
		Challenge string `json:"challenge,omitempty"`
		Status    string `json:"status,omitempty"`
	}

	authResp struct {
		Authorize authorize `json:"authorize,omitempty"`
		Streams   map[string]*stream
	}

	process struct {
		AccessToken   string `json:"access_token,omitempty"`
		Process       string `json:"process,omitempty"`
		TargetProfile string `json:"target_profile,omitempty"`
	}

	stream struct {
		Name      string     `json:"name,omitempty"`
		Online    int        `json:"online,omitempty"`
		Source    string     `json:"source,omitempty"`
		Processes []*process `json:"processes,omitempty"`
	}

	authReq struct {
		Username string `json:"username,omitempty"`
		Password string `json:"password,omitempty"`
	}

	addStreamReq struct {
		Minimal   int                `json:"minimal,omitempty"`
		Addstream map[string]*stream `json:"addstream,omitempty"`
	}

	deleteStreamReq struct {
		Authorize    *authReq `json:"authorize,omitempty"`
		Minimal      int      `json:"minimal,omitempty"`
		Deletestream []string `json:"deletestream,omitempty"`
	}
)

// NewMist creates new MistAPI object
func NewMist(host, login, password, livepeerToken string) *API {
	url := fmt.Sprintf("http://%s:4242/api2", host)
	return &API{host: host, login: login, password: password, apiURL: url,
		livepeerToken: livepeerToken,
	}
}

// Login authenicates to the Mist server
func (mapi *API) Login() {
	// u := mapi.apiURL + "?" + url.QueryEscape()
	resp, err := httpClient.Do(uhttp.GetRequest(mapi.apiURL))
	if err != nil {
		glog.Fatalf("Error authenticating to Mist server (%s) error: %v", mapi.apiURL, err)
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		glog.Fatalf("===== status error contacting Mist server (%s) status %d body: %s", mapi.apiURL, resp.StatusCode, string(b))
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.Fatalf("Error authenticating to Mist server (%s) error: %v", mapi.apiURL, err)
	}
	glog.Info(string(b))
	auth := &authResp{}
	err = json.Unmarshal(b, auth)
	if err != nil {
		panic(err)
	}
	glog.Infof("challenge: %s, status: %s", auth.Authorize.Challenge, auth.Authorize.Status)
	if auth.Authorize.Status != "CHALL" {
		glog.Fatalf("Unexpected status: %s", auth.Authorize.Status)
	}

	mh := md5.New()
	h1 := md5.Sum([]byte(mapi.password))
	mh.Write([]byte(hex.EncodeToString(h1[:])))
	mh.Write([]byte(auth.Authorize.Challenge))

	hashRes := hex.EncodeToString(mh.Sum(nil))
	glog.Infof("Challenge response: %s", hashRes)
	mapi.challenge = auth.Authorize.Challenge
	mapi.challengeRepsonse = hashRes
	// mac := hmac.New(md5.New, []byte(secret))
	// mac.Write([]byte(message))
	// expectedMAC := mac.Sum(nil)
	// return hex.EncodeToString(expectedMAC)

	u := mapi.apiURL + "?minimal=1&command=" + url.QueryEscape(fmt.Sprintf(`{"authorize":{"username":"%s","password":"%s"}}`, mapi.login, hashRes))
	resp, err = httpClient.Do(uhttp.GetRequest(u))
	if err != nil {
		glog.Fatalf("Error authenticating to Mist server (%s) error: %v", u, err)
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		glog.Fatalf("===== status error contacting Mist server (%s) status %d body: %s", u, resp.StatusCode, string(b))
	}
	b, err = ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		glog.Fatalf("Error authenticating to Mist server (%s) error: %v", mapi.apiURL, err)
	}
	glog.Info(string(b))
}

// CreateStream creates new stream in Mist server
func (mapi *API) CreateStream(name, profile string) {
	glog.Infof("Creating Mist stream '%s' with profile '%s'", name, profile)
	reqs := &addStreamReq{
		Minimal:   1,
		Addstream: make(map[string]*stream),
	}
	reqs.Addstream[name] = &stream{
		Name:      name,
		Source:    "push://",
		Processes: []*process{{Process: "Livepeer", AccessToken: mapi.livepeerToken, TargetProfile: profile}},
	}
	mapi.post(reqs)
}

// DeleteStreams removes streams from Mist server
func (mapi *API) DeleteStreams(names ...string) {
	glog.Infof("Deleting Mist streams '%v'", names)
	reqs := &deleteStreamReq{
		Authorize: &authReq{
			Username: mapi.login,
			Password: mapi.challengeRepsonse,
		},
		Minimal:      1,
		Deletestream: make([]string, 0),
	}
	for _, s := range names {
		reqs.Deletestream = append(reqs.Deletestream, s)
	}
	mapi.post(reqs)
}

func (mapi *API) post(commandi interface{}) []byte {
	cmdb, err := json.Marshal(commandi)
	if err != nil {
		panic(err)
	}
	command := string(cmdb)
	glog.Infof("Sending request %s", command)
	params := url.Values{}
	params.Add("command", command)
	body := strings.NewReader(params.Encode())
	// req := uhttp.RequireRequest(mapi.apiURL, "application/json", body)
	req := uhttp.RequireRequest("POST", mapi.apiURL, body)
	req.Header.Add("Content-Type", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		glog.Fatalf("Error sending Mist command %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		glog.Fatalf("Status error sending Mist command status %d", resp.StatusCode)
	}
	b, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		glog.Fatalf("Error sending Mist command %v", err)
	}
	glog.Info("Mist response: " + string(b))

	/*
		auth := &authResp{}
		err = json.Unmarshal(b, auth)
		if err != nil {
			return b
		}
		glog.Infof("challenge: %s, status: %s", auth.Authorize.Challenge, auth.Authorize.Status)
		if auth.Authorize.Status == "CHALL" {
			// need to re-authenticate
		}
	*/
	return b
}
