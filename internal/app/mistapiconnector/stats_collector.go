package mistapiconnector

import (
	"context"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/stream-tester/apis/mist"
)

type statsCollector struct {
	ctx  context.Context
	mapi *mist.API
}

func (c *statsCollector) Loop() {
	for range time.NewTicker(10 * time.Second).C {
		mistStats, err := c.mapi.GetStats()
		if err != nil {
			glog.Errorf("Error getting mist stats err=%v", err)
		}

		stats := compileStreamStats(mistStats)
		// TODO: Send events from those infos to rabbit
		_ = stats
	}
}

type streamFullStats struct {
	stream *mist.StreamStats
	pushes []*mist.Push
}

func compileStreamStats(mistStats *mist.MistStats) map[string]*streamFullStats {
	streamsStats := map[string]*streamFullStats{}
	getStats := func(stream string) *streamFullStats {
		if stats, ok := streamsStats[stream]; ok {
			return stats
		}
		stats := &streamFullStats{}
		streamsStats[stream] = stats
		return stats
	}

	for stream, stats := range mistStats.StreamsStats {
		getStats(stream).stream = stats
	}
	for _, push := range mistStats.PushList {
		info := getStats(push.Stream)
		info.pushes = append(info.pushes, push)
	}
	return streamsStats
}
