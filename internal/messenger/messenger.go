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
)

var (
	webhookURL string
	userName   string
)

type discordMessage struct {
	Content  string `json:"content,omitempty"`
	UserName string `json:"username,omitempty"`
}

// Init ...
func Init(WebhookURL, UserName string) {
	webhookURL = WebhookURL
	userName = UserName
}

// SendMessage send message to Discord channel
func SendMessage(msg string) {
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
