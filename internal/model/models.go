package model

import (
	"time"

	"golang.org/x/text/message"
)

const (
	SHORT   = 4
	DEBUG   = 5
	VERBOSE = 6
)

// ProfilesNum number of transcoding profiles
var ProfilesNum = 2

// Streamer interface
type Streamer interface {
	StartStreams(sourceFileName, host, rtmpPort, mediaPort string, simStreams, repeat uint, streamDuration time.Duration, notFinal bool) error
	Stats() *Stats
	StatsFormatted() string
	Done() <-chan struct{}
	Stop() // Stop active streams
	Cancel()
}

// Stats represents global test statistics
type Stats struct {
	RTMPActiveStreams            int     `json:"rtmp_active_streams"` // number of active RTMP streams
	RTMPstreams                  int     `json:"rtm_pstreams"`        // number of RTMP streams
	MediaStreams                 int     `json:"media_streams"`       // number of media streams
	TotalSegmentsToSend          int     `json:"total_segments_to_send"`
	SentSegments                 int     `json:"sent_segments"`
	DownloadedSegments           int     `json:"downloaded_segments"`
	ShouldHaveDownloadedSegments int     `json:"should_have_downloaded_segments"`
	FailedToDownloadSegments     int     `json:"failed_to_download_segments"`
	BytesDownloaded              int64   `json:"bytes_downloaded"`
	Retries                      int     `json:"retries"`
	SuccessRate                  float64 `json:"success_rate"` // DownloadedSegments/profilesNum*SentSegments
	ConnectionLost               int     `json:"connection_lost"`
	Finished                     bool    `json:"finished"`
	ProfilesNum                  int     `json:"profiles_num,omitempty"`
}

// REST requests

// StartStreamsReq start streams request
type StartStreamsReq struct {
	FileName        string `json:"file_name,omitempty"`
	Host            string `json:"host,omitempty"`
	RTMP            int    `json:"rtmp,omitempty"`
	Media           int    `json:"media,omitempty"`
	Repeat          uint   `json:"repeat,omitempty"`
	Simultaneous    uint   `json:"simultaneous,omitempty"`
	Time            string `json:"time,omitempty"`
	ProfilesNum     int    `json:"profiles_num,omitempty"`
	DoNotClearStats bool   `json:"do_not_clear_stats"`
}

// FormatForConsole formats stats to be shown in console
func (st *Stats) FormatForConsole() string {
	p := message.NewPrinter(message.MatchLanguage("en"))
	r := p.Sprintf(`
Number of RTMP streams:                       %7d
Number of media streams:                      %7d
Total number of segments to be sent:          %7d
Total number of segments sent to broadcaster: %7d
Total number of segments read back:           %7d
Total number of segments should read back:    %7d
Number of retries:                            %7d
Success rate:                                     %9.5f%%
Lost connection to broadcaster:               %7d
Bytes dowloaded:                         %12d`, st.RTMPstreams, st.MediaStreams, st.TotalSegmentsToSend, st.SentSegments, st.DownloadedSegments,
		st.ShouldHaveDownloadedSegments, st.Retries, st.SuccessRate, st.ConnectionLost, st.BytesDownloaded)
	return r
}
