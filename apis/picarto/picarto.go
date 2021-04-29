// Package picarto API
package picarto

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/stream-tester/internal/utils/uhttp"
	"github.com/livepeer/stream-tester/model"
)

const httpTimeout = 4 * time.Second

var httpClient = &http.Client{
	// Transport: &http2.Transport{TLSClientConfig: tlsConfig},
	// Transport: &http2.Transport{AllowHTTP: true},
	Timeout: httpTimeout,
}

const (
	apiURL = "https://api.picarto.tv/api/v1/"
)

type (

	// UserProfile user proflie
	UserProfile struct {
		UserID      int               `json:"user_id,omitempty"`
		Name        string            `json:"name,omitempty"`
		Title       string            `json:"title,omitempty"`
		Viewers     int               `json:"viewers,omitempty"`
		Thumbnails  map[string]string `json:"thumbnails,omitempty"`
		Category    string            `json:"category,omitempty"`
		AccountType string            `json:"account_type,omitempty"` // free
		Adult       bool              `json:"adult,omitempty"`
		Gaming      bool              `json:"gaming,omitempty"`
		Commissions bool              `json:"commissions,omitempty"`
		Multistream bool              `json:"multistream,omitempty"`
		Languages   []struct {
			ID   int    `json:"id,omitempty"`
			Name string `json:"name,omitempty"`
		} `json:"languages,omitempty"`
	}
)

// GetOnlineUsers returns list of online users
func GetOnlineUsers(country string, adult, gaming bool) ([]UserProfile, error) {
	u := apiURL + "online?"
	values := make(url.Values)
	if adult {
		values.Add("adult", "true")
	}
	if gaming {
		values.Add("gaming", "true")
	}
	u += values.Encode()
	resp, err := httpClient.Do(uhttp.GetRequest(u))
	if err != nil {
		glog.Errorf("Error requesting Picarto online users (%s) error: %v", u, err)
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		glog.Errorf("===== status error contacting Picarto server (%s) status %d body: %s", u, resp.StatusCode, string(b))
		return nil, fmt.Errorf("Status error %s", resp.Status)
	}
	b, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		glog.Errorf("Error requesting Picarto online users (%s) error: %v", u, err)
		return nil, err
	}
	glog.V(model.INSANE2).Info(string(b))
	var users []UserProfile
	err = json.Unmarshal(b, &users)
	if err != nil || country == "" {
		return users, err
	}
	var rusers []UserProfile
	for _, u := range users {
		if strings.Contains(u.Thumbnails["web"], country) {
			rusers = append(rusers, u)
		}
	}
	return rusers, nil
}
