package model

import "golang.org/x/text/message"

const (
	SHORT   = 4
	DEBUG   = 5
	VERBOSE = 6

	profilesNum = 2 // number of transcoding profiles
)

// M3UTester Downloads playlist and segments, counts statistcs
type M3UTester interface {
	Start(url string)
	StatsFormatted() string
}

// Streamer interface
type Streamer interface {
	StartStreams(sourceFileName, host, rtmpPort, mediaPort string, number uint) error
	Stats() *Stats
	StatsFormatted() string
	Done() <-chan struct{}
}

// Stats ...
type Stats struct {
	RTMPstreams              int // number of RTMP streams
	MediaStreams             int // number of media streams
	SentSegments             int
	DownloadedSegments       int
	FailedToDownloadSegments int
	BytesDownloaded          int64
	SucessRate               float64 // DownloadedSegments/profilesNum*SentSegments
}

// FormatForConsole ...
func (st *Stats) FormatForConsole() string {
	p := message.NewPrinter(message.MatchLanguage("en"))
	var successRate float64
	if st.SentSegments > 0 {
		successRate = float64(st.DownloadedSegments) / ((profilesNum + 1) * float64(st.SentSegments)) * 100
	}
	// r := fmt.Sprintf(`
	r := p.Sprintf(`
Number of RTMP streams:                       %7d
Number of media streams:                      %7d
Total number of segments sent to broadcaster: %7d
Total number of segments read back:           %7d
Success rate:                                     %9.5f%%
Bytes dowloaded:                         %12d`, st.RTMPstreams, st.MediaStreams, st.SentSegments, st.DownloadedSegments, successRate, st.BytesDownloaded)
	return r
}
