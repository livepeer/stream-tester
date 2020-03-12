/*
Package messenger sends messages to Discord channel
*/
package messenger

import (
	"bytes"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"encoding/json"

	"github.com/golang/glog"
	"github.com/livepeer/stream-tester/model"
	"github.com/patrickmn/go-cache"
)

var (
	webhookURL    string
	userName      string
	usersToNotify string
	debounceCache *cache.Cache = cache.New(5*time.Minute, 30*time.Minute)
	msgCh         chan string
)

type discordMessage struct {
	Content  string `json:"content,omitempty"`
	UserName string `json:"username,omitempty"`
}

// Init ...
func Init(WebhookURL, UserName, UsersToNotify string) {
	webhookURL = WebhookURL
	userName = UserName
	usersToNotify = UsersToNotify
	if WebhookURL != "" {
		msgCh = make(chan string, 64)
		go sendLoop()
	}
}

func sendLoop() {
	var msgQueue []string
	var goodAfter time.Time
	var headers http.Header
	var status int
	timer := time.NewTimer(2 * time.Second)
	var step int
	for {
		glog.V(model.INSANE).Infof("====> sendLoop step %d queue len %d", step, len(msgQueue))
		step++
		select {
		case <-timer.C:
			if len(msgQueue) == 0 {
				continue
			}
			msg := msgQueue[0]
			status, headers = postMessage(msg)
			if headers == nil || (status != http.StatusNoContent && status != http.StatusOK) {
				// error possibly
				// timer.Reset(2 * time.Second)
				if headers != nil && status == http.StatusTooManyRequests {
					rafters := headers.Get("Retry-After")
					if rafters != "" {
						rafter, _ := strconv.ParseInt(rafters, 10, 64)
						timer = time.NewTimer(time.Duration(rafter) * time.Millisecond)
						continue
					}
				}
				timer = time.NewTimer(2 * time.Second)
				continue
			}
			msgQueue = msgQueue[1:]

		case msg := <-msgCh:
			if len(msgQueue) > 0 || time.Now().Before(goodAfter) {
				msgQueue = append(msgQueue, msg)
				if len(msgQueue) > 128 {
					msgQueue = msgQueue[1:]
				}
				continue
			}
			status, headers = postMessage(msg)
			if headers == nil || (status != http.StatusNoContent && status != http.StatusOK) {
				// error possibly
				msgQueue = append(msgQueue, msg)
				if headers != nil && status == http.StatusTooManyRequests {
					rafters := headers.Get("Retry-After")
					if rafters != "" {
						rafter, _ := strconv.ParseInt(rafters, 10, 64)
						timer = time.NewTimer(time.Duration(rafter) * time.Millisecond)
						continue
					}
				}
				// if !timer.Stop() {
				// 	<-timer.C
				// }
				// timer.Reset(2 * time.Second)
				timer = time.NewTimer(2 * time.Second)
				// glog.Infof("Reset for 2s done")
				continue
			}
		}
		rlrem := headers.Get("X-Ratelimit-Remaining")
		if rlrem == "" { // shoudn't happen
			continue
		}
		if rlrem == "0" {
			rlreset := headers.Get("X-Ratelimit-Reset-After")
			frlreset, err := strconv.ParseFloat(rlreset, 64)
			if err != nil {
				panic(err)
				continue
			}
			wait := time.Duration(frlreset*1000.0+100.0) * time.Millisecond
			glog.V(model.VVERBOSE).Infof("Need wait %s", wait)
			goodAfter = time.Now().Add(wait)
			// timer.Reset(wait)
			timer = time.NewTimer(wait)
			// glog.Infof("Reset for %s done", wait)
			/*
				rlreset := headers.Get("X-Ratelimit-Reset")
				if rlreset == "" {
					continue
				}
				frlreset, err := strconv.ParseFloat(rlreset, 64)
				if err != nil {
					continue
				}
				nextTime := time.Unix(0, order.Created*int64(time.Millisecond))
			*/

		} else if len(msgQueue) > 0 {
			// glog.Infof("Queue not empty")
			// if !timer.Stop() {
			// 	<-timer.C
			// }
			// timer.Reset(50 * time.Millisecond)
			timer = time.NewTimer(50 * time.Millisecond)
		}
	}
}

// SendFatalMessage send message to Discord channel
// and automatically mentiones UsersToNotify in the message
func SendFatalMessage(msg string) {
	glog.Error(msg)
	if usersToNotify != "" {
		msg = usersToNotify + ": " + msg
	}
	sendMessage(msg)
}

// SendMessage send message to Discord channel
func SendMessage(msg string) {
	if msg == "" {
		return
	}
	glog.Info(msg)
	sendMessage(msg)
}

// SendCodeMessage send message to Discord channel, wrapping it as three ticks
func SendCodeMessage(msg string) {
	if msg == "" {
		return
	}
	glog.Info(msg)
	sendMessage("```\n" + msg + "```")
}

// SendMessageDebounced send message to Discord channel
func SendMessageDebounced(msg string) {
	glog.Info(msg)
	if _, has := debounceCache.Get(msg); !has {
		sendMessage(msg)
		debounceCache.SetDefault(msg, true)
	}
}

func sendMessage(msg string) {
	if webhookURL == "" || msgCh == nil {
		return
	}
	if len(msg) > 2000 {
		for {
			l := 1980
			if l > len(msg) {
				l = len(msg)
			}
			msg1 := msg[:l]
			msg = msg[l:]
			sendMessage(msg1)
			if len(msg) == 0 {
				break
			}
		}
		return
	}
	msgCh <- msg
}

func postMessage(msg string) (int, http.Header) {
	if webhookURL == "" {
		return 0, nil
	}
	dm := &discordMessage{
		Content:  msg,
		UserName: userName,
	}
	data, _ := json.Marshal(dm)
	var body io.Reader
	body = bytes.NewReader(data)
	// resp, err := http.Post(webhookURL, "application/json", body)
	req, _ := http.NewRequest("POST", webhookURL, body)
	req.Header.Add("User-Agent", "stream-tester/"+model.Version)
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("X-RateLimit-Precision", "millisecond")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		glog.Errorf("error posting to Discord err=%v", err)
		return 0, nil
	}
	b, _ := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	glog.V(model.INSANE).Infof("Discord response headers")
	for k, v := range resp.Header {
		glog.V(model.INSANE).Infof("%s: %+v", k, v)
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		glog.Errorf("status error posting to Discord status=%s body: %s", resp.Status, string(b))
	}
	return resp.StatusCode, resp.Header
}
