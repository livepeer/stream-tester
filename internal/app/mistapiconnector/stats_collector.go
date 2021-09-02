package mistapiconnector

import (
	"context"
	"fmt"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/livepeer-data/pkg/data"
	"github.com/livepeer/livepeer-data/pkg/event"
	"github.com/livepeer/stream-tester/apis/mist"
)

type statsCollector struct {
	ctx      context.Context
	nodeID   string
	mapi     *mist.API
	producer *event.AMQPProducer
}

type infoProvider interface {
	getStreamInfo(mistID string) *streamInfo
}

func (c *statsCollector) mainLoop(period time.Duration, infop infoProvider) {
	ticker := time.NewTicker(period)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
		}
		mistStats, err := c.mapi.GetStats()
		if err != nil {
			glog.Errorf("Error getting mist stats err=%v", err)
		}

		streamsStats := compileStreamStats(mistStats)
		// TODO: Send events from those infos to rabbit
		for stream, stats := range streamsStats {
			info := infop.getStreamInfo(stream)
			info.mu.Lock()
			msStats := make([]*data.MultistreamTargetStats, len(stats.pushes))
			for i, push := range stats.pushes {
				var targetStats *data.MultistreamStats
				if push.Stats != nil {
					targetStats = &data.MultistreamStats{
						ActiveSec:   push.Stats.ActiveSeconds,
						Bytes:       push.Stats.Bytes,
						MediaTimeMs: push.Stats.MediaTime,
					}
				}
				pushInfo := info.pushStatus[push.OriginalURI]
				msStats[i] = &data.MultistreamTargetStats{
					Target: pushToMultistreamTargetInfo(pushInfo),
					Stats:  targetStats,
				}
			}
			msssEvent := data.NewMistStreamStatsEvent(c.nodeID, info.stream.ID, stats.stream.Clients, stats.stream.MediaTimeMs, msStats)
			// TODO: Per send timeouts? Per iteration?
			c.producer.Publish(c.ctx, event.AMQPMessage{
				Exchange: ownExchangeName,
				Key:      fmt.Sprintf("stream_stats.%d", info.stream.ID),
				Body:     msssEvent,
			})
		}
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
