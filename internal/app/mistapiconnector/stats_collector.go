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

type infoProvider interface {
	getStreamInfo(mistID string) *streamInfo
}

type statsCollector struct {
	nodeID       string
	mapi         *mist.API
	producer     *event.AMQPProducer
	amqpExchange string
	infoProvider
}

func startStatsCollector(ctx context.Context, period time.Duration, nodeID string, mapi *mist.API, producer *event.AMQPProducer, amqpExchange string, infop infoProvider) {
	sc := &statsCollector{nodeID, mapi, producer, amqpExchange, infop}
	go sc.mainLoop(ctx, period)
}

func (c *statsCollector) mainLoop(loopCtx context.Context, period time.Duration) {
	ticker := time.NewTicker(period)
	defer ticker.Stop()
	for {
		select {
		case <-loopCtx.Done():
			return
		case <-ticker.C:
		}
		ctx, cancel := context.WithTimeout(loopCtx, period)
		mistStats, err := c.mapi.GetStats()
		if err != nil {
			glog.Errorf("Error getting mist stats. err=%v", err)
			cancel()
			continue
		}

		streamsStats := compileStreamStats(mistStats)
		for stream, stats := range streamsStats {
			// TODO: Handle when `info` may be null here
			info := c.getStreamInfo(stream)
			mssEvent := createStatsEvent(c.nodeID, info, stats)
			err := c.producer.Publish(ctx, event.AMQPMessage{
				Exchange: c.amqpExchange,
				Key:      fmt.Sprintf("stream_stats.%s", info.stream.ID),
				Body:     mssEvent,
			})
			if err != nil {
				glog.Errorf("Error sending mist stats event. err=%q streamId=%q event=%+v", err, info.stream.ID, mssEvent)
				if ctx.Err() != nil {
					break
				}
			}
		}
		cancel()
	}
}

func createStatsEvent(nodeID string, info *streamInfo, stats *streamFullStats) *data.MistStreamStatsEvent {
	info.mu.Lock()
	defer info.mu.Unlock()
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
	// TODO: Handle when `stats.stream` has some dummy values like 0 and -1, when
	// the stream is not active anymore.
	return data.NewMistStreamStatsEvent(nodeID, info.stream.ID, stats.stream.Clients, stats.stream.MediaTimeMs, msStats)
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
