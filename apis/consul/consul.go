// Package consul API
package consul

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/stream-tester/internal/utils/uhttp"
	"github.com/livepeer/stream-tester/model"
)

// GetKeyResponse response from Consul
type GetKeyResponse struct {
	LockIndex   int64
	Key         string
	Flags       int64
	Value       string
	CreateIndex int64
	ModifyIndex int64
}

// ErrNotFound returned if key is not found
var ErrNotFound = errors.New("Key not found")
var ErrConfilct = errors.New("Conflict")

const httpTimeout = 2 * time.Second

// GetKey retrieves key from Consul's KV storage
func GetKey(u *url.URL, path string) (string, error) {
	var cu url.URL = *u
	cu.Path = "v1/kv/" + path
	cu.RawQuery = "raw"
	glog.Infof("Making GET request to %s", cu.String())
	ctx, cancel := context.WithTimeout(context.Background(), httpTimeout)
	resp, err := http.DefaultClient.Do(uhttp.NewRequestWithContext(ctx, "GET", cu.String(), nil))
	cancel()
	if err != nil {
		glog.Errorf("Error getting key '%s' from Consul at %s error: %v", path, cu.String(), err)
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := ioutil.ReadAll(resp.Body)
		glog.Errorf("Status error contacting Consul (%s) status %d body: %s", cu.String(), resp.StatusCode, string(b))
		if resp.StatusCode == http.StatusNotFound {
			return "", ErrNotFound
		}
		return "", errors.New(http.StatusText(resp.StatusCode))
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.Errorf("Error reading from Consul (%s) error: %v", cu.String(), err)
		return "", err
	}
	val := string(b)
	glog.Infof("Read from Consul '%s': '%s'", path, val)
	return val, nil
}

// GetKeyEx retrieves key from Consul's KV storage
func GetKeyEx(u *url.URL, path string, recurse bool) ([]GetKeyResponse, error) {
	var cu url.URL = *u
	cu.Path = "v1/kv/" + path
	q := make(url.Values)
	if recurse {
		q.Add("recurse", "true")
	}
	cu.RawQuery = q.Encode()
	// cu.RawQuery = "raw"
	glog.Infof("Making GET request to %s", cu.String())
	ctx, cancel := context.WithTimeout(context.Background(), httpTimeout)
	resp, err := http.DefaultClient.Do(uhttp.NewRequestWithContext(ctx, "GET", cu.String(), nil))
	cancel()
	if err != nil {
		glog.Errorf("Error getting key '%s' from Consul at %s error: %v", path, cu.String(), err)
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := ioutil.ReadAll(resp.Body)
		glog.Errorf("Status error contacting Consul (%s) status %d body: %s", cu.String(), resp.StatusCode, string(b))
		if resp.StatusCode == http.StatusNotFound {
			return nil, ErrNotFound
		}
		return nil, errors.New(http.StatusText(resp.StatusCode))
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.Errorf("Error reading from Consul (%s) error: %v", cu.String(), err)
		return nil, err
	}
	var gkr []GetKeyResponse
	err = json.Unmarshal(b, &gkr)
	if err != nil {
		return nil, err
	}
	val := string(b)
	glog.Infof("Read from Consul '%s': '%s'", path, val)
	return gkr, nil
}

// PutKey set key to Consul's KV storage
func PutKey(u *url.URL, path, value string) error {
	var cu url.URL = *u
	cu.Path = "v1/kv/" + path
	glog.V(model.VERBOSE).Infof("Making PUT request to %s", cu.String())
	var body io.Reader
	body = bytes.NewReader([]byte(value))
	ctx, cancel := context.WithTimeout(context.Background(), httpTimeout)
	resp, err := http.DefaultClient.Do(uhttp.NewRequestWithContext(ctx, "PUT", cu.String(), body))
	cancel()
	if err != nil {
		glog.Errorf("Error putting key '%s' to Consul at %s error: %v", path, cu.String(), err)
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := ioutil.ReadAll(resp.Body)
		glog.Errorf("Status error contacting Consul (%s) status %d body: %s", cu.String(), resp.StatusCode, string(b))
		return errors.New(resp.Status + ": " + string(b))
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.Errorf("Error reading from Consul (%s) error: %v", cu.String(), err)
		return err
	}
	val := string(b)
	glog.V(model.VERBOSE).Infof("Read from Consul '%s': '%s'", path, val)
	return nil
}

// PutKeysWithCurrentTime puts keys in one transaction and sets Flags
// field to the current timestamp (in ms)
func PutKeysWithCurrentTime(u *url.URL, kvs ...string) error {
	if len(kvs) == 0 || len(kvs)%2 != 0 {
		return errors.New("Number of arguments should be even")
	}
	now := time.Now().UnixNano() / int64(time.Millisecond)
	ks := make([]GetKeyResponse, 0, len(kvs)/2)
	for i := 0; i < len(kvs); i += 2 {
		ks = append(ks, GetKeyResponse{Key: kvs[i], Value: kvs[i+1], Flags: now})
	}
	return PutKeysEx(u, ks)
}

// PutKeys puts keys in one transaction
func PutKeys(u *url.URL, kvs ...string) error {
	if len(kvs) == 0 || len(kvs)%2 != 0 {
		return errors.New("Number of arguments should be even")
	}
	ks := make([]GetKeyResponse, 0, len(kvs)/2)
	for i := 0; i < len(kvs); i += 2 {
		ks = append(ks, GetKeyResponse{Key: kvs[i], Value: kvs[i+1]})
	}
	return PutKeysEx(u, ks)
}

// PutKeysEx puts keys in one transaction
func PutKeysEx(u *url.URL, ks []GetKeyResponse) error {
	if len(ks) == 0 {
		return errors.New("Number of arguments should be greater than zero")
	}
	var cu url.URL = *u
	cu.Path = "v1/txn"
	glog.V(model.VERBOSE).Infof("Making transaction PUT request to %s", cu.String())
	var body io.Reader
	bodyParts := make([]string, 0, len(ks))
	for _, kvi := range ks {
		val := base64.StdEncoding.EncodeToString([]byte(kvi.Value))
		bodyParts = append(bodyParts, fmt.Sprintf(`{"KV":{"Verb":"set", "Key": "%s", "Value": "%s", "Flags": %d}}`, kvi.Key, val, kvi.Flags))
	}
	bodyStr := `[` + strings.Join(bodyParts, ",") + `]`
	body = bytes.NewReader([]byte(bodyStr))
	glog.V(model.VVERBOSE).Infof("Making transaction PUT request to %s body: '%s'", cu.String(), bodyStr)

	ctx, cancel := context.WithTimeout(context.Background(), httpTimeout)
	resp, err := http.DefaultClient.Do(uhttp.NewRequestWithContext(ctx, "PUT", cu.String(), body))
	cancel()
	if err != nil {
		glog.Errorf("Error putting keys '%s' to Consul at %s error: %v", ks[0].Key, cu.String(), err)
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := ioutil.ReadAll(resp.Body)
		glog.Errorf("Status error contacting Consul (%s) status %d body: %s", cu.String(), resp.StatusCode, string(b))
		return errors.New(resp.Status + ": " + string(b))
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.Errorf("Error reading from Consul (%s) error: %v", cu.String(), err)
		return err
	}
	val := string(b)
	glog.V(model.VERBOSE).Infof("Put keys result '%s': '%s'", ks[0].Key, val)
	return nil
}

// DeleteKey retrieves key from Consul's KV storage
func DeleteKey(u *url.URL, path string, recurse bool) (bool, error) {
	var cu url.URL = *u
	cu.Path = "v1/kv/" + path
	q := make(url.Values)
	if recurse {
		q.Add("recurse", "true")
	}
	cu.RawQuery = q.Encode()
	glog.V(model.VERBOSE).Infof("Making DELETE request to %s", cu.String())
	ctx, cancel := context.WithTimeout(context.Background(), httpTimeout)
	resp, err := http.DefaultClient.Do(uhttp.NewRequestWithContext(ctx, "DELETE", cu.String(), nil))
	cancel()
	if err != nil {
		glog.Errorf("Error deleting key '%s' from Consul at %s error: %v", path, cu.String(), err)
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := ioutil.ReadAll(resp.Body)
		glog.Errorf("Status error contacting Consul (%s) status %d body: %s", cu.String(), resp.StatusCode, string(b))
		if resp.StatusCode == http.StatusNotFound {
			return false, ErrNotFound
		}
		return false, errors.New(http.StatusText(resp.StatusCode))
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.Errorf("Error reading from Consul (%s) error: %v", cu.String(), err)
		return false, err
	}
	val := string(b)
	glog.V(model.VERBOSE).Infof("Delete result key=%s res=%s", path, val)
	return strings.TrimSpace(val) == "true", nil
}

// DeleteKeysCas delete keys from Consul's KV storage
func DeleteKeysCas(u *url.URL, ks []GetKeyResponse) (bool, error) {
	if len(ks) == 0 {
		return false, errors.New("Number of arguments should be greater than zero")
	}
	var cu url.URL = *u
	cu.Path = "v1/txn"
	glog.V(model.VERBOSE).Infof("Making transaction PUT request to %s", cu.String())
	var body io.Reader
	bodyParts := make([]string, 0, len(ks))
	for _, kvi := range ks {
		bodyParts = append(bodyParts, fmt.Sprintf(`{"KV":{"Verb":"delete-cas", "Key": "%s", "Index": %d}}`, kvi.Key, kvi.ModifyIndex))
	}
	bodyStr := `[` + strings.Join(bodyParts, ",") + `]`
	body = bytes.NewReader([]byte(bodyStr))
	glog.V(model.VVERBOSE).Infof("Making transaction PUT request to %s body: '%s'", cu.String(), bodyStr)

	ctx, cancel := context.WithTimeout(context.Background(), httpTimeout)
	resp, err := http.DefaultClient.Do(uhttp.NewRequestWithContext(ctx, "PUT", cu.String(), body))
	cancel()
	if err != nil {
		glog.Errorf("Error deleting keys '%s' to Consul at %s error: %v", ks[0].Key, cu.String(), err)
		return false, err
	}
	defer resp.Body.Close()
	b, _ := ioutil.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusConflict {
		return false, ErrConfilct
	}
	if resp.StatusCode != http.StatusOK {
		glog.Errorf("Status error contacting Consul (%s) status %d body: %s", cu.String(), resp.StatusCode, string(b))
		return false, errors.New(resp.Status + ": " + string(b))
	}
	if err != nil {
		glog.Errorf("Error reading response from Consul (%s) error: %v", cu.String(), err)
		return false, err
	}
	val := string(b)
	glog.V(model.VERBOSE).Infof("Delete keys result '%s': '%s'", ks[0].Key, val)
	return true, nil
}
