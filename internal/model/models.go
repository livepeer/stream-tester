package model

import (
	"fmt"
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

// InfinitePuller interface
type InfinitePuller interface {
	// Start blocks
	Start()
}

// Streamer2 interface
type Streamer2 interface {
	StartStreaming(sourceFileName string, rtmpIngestURL, mediaURL string, waitForTarget time.Duration)
	// StartPulling pull arbitrary HLS stream and report found errors
	StartPulling(mediaURL string)
}

// Streamer interface
type Streamer interface {
	StartStreams(sourceFileName, bhost, rtmpPort, mhost, mediaPort string, simStreams, repeat uint, streamDuration time.Duration,
		notFinal, measureLatency, noBar bool, groupStartBy int, startDelayBetweenGroups, waitForTarget time.Duration) (string, error)
	Stats(basedManifestID string) *Stats
	StatsFormatted() string
	// DownStatsFormatted() string
	// AnalyzeFormatted(short bool) string
	Done() <-chan struct{}
	Stop() // Stop active streams
	Cancel()
}

// Latencies contains latencies
type Latencies struct {
	Avg time.Duration `json:"avg"`
	P50 time.Duration `json:"p_50"`
	P95 time.Duration `json:"p_95"`
	P99 time.Duration `json:"p_99"`
}

// Stats represents global test statistics
type Stats struct {
	RTMPActiveStreams            int             `json:"rtmp_active_streams"` // number of active RTMP streams
	RTMPstreams                  int             `json:"rtmp_streams"`        // number of RTMP streams
	MediaStreams                 int             `json:"media_streams"`       // number of media streams
	TotalSegmentsToSend          int             `json:"total_segments_to_send"`
	SentSegments                 int             `json:"sent_segments"`
	DownloadedSegments           int             `json:"downloaded_segments"`
	ShouldHaveDownloadedSegments int             `json:"should_have_downloaded_segments"`
	FailedToDownloadSegments     int             `json:"failed_to_download_segments"`
	BytesDownloaded              int64           `json:"bytes_downloaded"`
	Retries                      int             `json:"retries"`
	SuccessRate                  float64         `json:"success_rate"` // DownloadedSegments/profilesNum*SentSegments
	ConnectionLost               int             `json:"connection_lost"`
	Finished                     bool            `json:"finished"`
	ProfilesNum                  int             `json:"profiles_num"`
	SourceLatencies              Latencies       `json:"source_latencies"`
	TranscodedLatencies          Latencies       `json:"transcoded_latencies"`
	RawSourceLatencies           []time.Duration `json:"raw_source_latencies"`
	RawTranscodedLatencies       []time.Duration `json:"raw_transcoded_latencies"`
	WowzaMode                    bool            `json:"wowza_mode"`
	Gaps                         int             `json:"gaps"`
	StartTime                    time.Time       `json:"start_time"`
	Errors                       map[string]int  `json:"errors"`
}

// REST requests

// StartStreamsReq start streams request
type StartStreamsReq struct {
	FileName        string `json:"file_name"`
	Host            string `json:"host"`
	MHost           string `json:"media_host"`
	RTMP            int    `json:"rtmp"`
	Media           int    `json:"media"`
	Repeat          uint   `json:"repeat"`
	Simultaneous    uint   `json:"simultaneous"`
	Time            string `json:"time"`
	ProfilesNum     int    `json:"profiles_num"`
	DoNotClearStats bool   `json:"do_not_clear_stats"`
	MeasureLatency  bool   `json:"measure_latency"`
	HTTPIngest      bool   `json:"http_ingest"`
}

// StartStreamsRes start streams response
type StartStreamsRes struct {
	Success        bool   `json:"success"`
	BaseManifestID string `json:"base_manifest_id"`
}

// FormatForConsole formats stats to be shown in console
func (st *Stats) FormatForConsole() string {
	p := message.NewPrinter(message.MatchLanguage("en"))
	r := p.Sprintf(`
Number of RTMP streams:                       %7d
Number of media streams:                      %7d
Started ago                                   %7s
Total number of segments to be sent:          %7d
Total number of segments sent to broadcaster: %7d
Total number of segments read back:           %7d
Total number of segments should read back:    %7d
Number of retries:                            %7d
Success rate:                                     %9.5f%%
Lost connection to broadcaster:               %7d
Source latencies:                             %s
Transcoded latencies:                         %s
Bytes dowloaded:                         %12d`, st.RTMPstreams, st.MediaStreams, time.Now().Sub(st.StartTime), st.TotalSegmentsToSend, st.SentSegments, st.DownloadedSegments,
		st.ShouldHaveDownloadedSegments, st.Retries, st.SuccessRate, st.ConnectionLost, st.SourceLatencies.String(), st.TranscodedLatencies.String(), st.BytesDownloaded)
	if len(st.Errors) > 0 {
		r = fmt.Sprintf("%s\nErrors: %+v\n", r, st.Errors)
	}
	return r
}

func (ls *Latencies) String() string {
	r := fmt.Sprintf(`{Average: %s, P50: %s, P95: %s, P99: %s}`, ls.Avg, ls.P50, ls.P95, ls.P99)
	return r
}
