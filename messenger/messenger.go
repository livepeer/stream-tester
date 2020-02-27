/*
Package messenger sends messages to Discord channel
*/
package messenger

import (
	"bytes"
	"io"
	"net/http"
	"time"

	"encoding/json"

	"github.com/golang/glog"
	"github.com/patrickmn/go-cache"
)

var (
	webhookURL    string
	userName      string
	usersToNotify string
	debounceCache *cache.Cache = cache.New(5*time.Minute, 30*time.Minute)
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
	if webhookURL == "" {
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
			SendMessage(msg1)
			if len(msg) == 0 {
				break
			}
			time.Sleep(time.Second)
		}
		return
	}
	dm := &discordMessage{
		Content:  msg,
		UserName: userName,
	}
	data, _ := json.Marshal(dm)
	var body io.Reader
	body = bytes.NewReader(data)
	http.Post(webhookURL, "application/json", body)
}
