package mistapiconnector

import (
	"context"
	"fmt"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/livepeer-data/pkg/data"
	"github.com/livepeer/livepeer-data/pkg/event"
	"github.com/livepeer/stream-tester/apis/mist"
	census "github.com/livepeer/stream-tester/internal/metrics"
)

type infoProvider interface {
	getStreamInfo(mistID string) (*streamInfo, error)
}

type metricsCollector struct {
	nodeID, ownRegion string
	mapi              *mist.API
	producer          *event.AMQPProducer
	amqpExchange      string
	infoProvider
}

func startMetricsCollector(ctx context.Context, period time.Duration, nodeID, ownRegion string, mapi *mist.API, producer *event.AMQPProducer, amqpExchange string, infop infoProvider) {
	mc := &metricsCollector{nodeID, ownRegion, mapi, producer, amqpExchange, infop}
	mc.collectMetricsLogged(ctx, period)
	go mc.mainLoop(ctx, period)
}

func (c *metricsCollector) mainLoop(loopCtx context.Context, period time.Duration) {
	ticker := time.NewTicker(period)
	defer ticker.Stop()
	for {
		select {
		case <-loopCtx.Done():
			return
		case <-ticker.C:
			c.collectMetricsLogged(loopCtx, period)
		}
	}
}

func (c *metricsCollector) collectMetricsLogged(ctx context.Context, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := c.collectMetrics(ctx); err != nil {
		glog.Errorf("Error collecting mist metrics. err=%v", err)
	}
}

func (c *metricsCollector) collectMetrics(ctx context.Context) error {
	defer func() {
		if rec := recover(); rec != nil {
			glog.Errorf("Panic in metrics collector. value=%v", rec)
		}
	}()

	mistStats, err := c.mapi.GetStats()
	if err != nil {
		return err
	}
	streamsMetrics := compileStreamMetrics(mistStats)

	for streamID, metrics := range streamsMetrics {
		info, err := c.getStreamInfo(streamID)
		if err != nil {
			return fmt.Errorf("error getting stream info for %s: %w", streamID, err)
		}
		if info == nil {
			glog.Infof("Mist exported metrics from unknown stream. streamId=%q metrics=%+v", streamID, metrics)
			continue
		}
		mseEvent := createMetricsEvent(c.nodeID, c.ownRegion, info, metrics)
		err = c.producer.Publish(ctx, event.AMQPMessage{
			Exchange: c.amqpExchange,
			Key:      fmt.Sprintf("stream.metrics.%s", info.stream.ID),
			Body:     mseEvent,
		})
		if err != nil {
			glog.Errorf("Error sending mist stream metrics event. err=%q streamId=%q event=%+v", err, info.stream.ID, mseEvent)
			if ctx.Err() != nil {
				return err
			}
		}
	}
	return nil
}

func createMetricsEvent(nodeID, region string, info *streamInfo, metrics *streamMetrics) *data.MediaServerMetricsEvent {
	info.mu.Lock()
	defer info.mu.Unlock()
	multistream := make([]*data.MultistreamTargetMetrics, len(metrics.pushes))
	for i, push := range metrics.pushes {
		pushInfo := info.pushStatus[push.OriginalURI]
		if pushInfo == nil {
			pushInfo = &pushStatus{}
			info.pushStatus[push.OriginalURI] = pushInfo
		}
		var metrics *data.MultistreamMetrics
		if push.Stats != nil {
			metrics = &data.MultistreamMetrics{
				ActiveSec:   push.Stats.ActiveSeconds,
				Bytes:       push.Stats.Bytes,
				MediaTimeMs: push.Stats.MediaTime,
			}
			if last := pushInfo.metrics; last != nil {
				if metrics.Bytes > last.Bytes {
					census.IncMultistreamBytes(metrics.Bytes-last.Bytes, info.stream.PlaybackID) // manifestID === playbackID
				}
				if metrics.MediaTimeMs > last.MediaTimeMs {
					diff := time.Duration(metrics.MediaTimeMs-last.MediaTimeMs) * time.Millisecond
					census.IncMultistreamTime(diff, info.stream.PlaybackID)
				}
			}
			pushInfo.metrics = metrics
		}
		multistream[i] = &data.MultistreamTargetMetrics{
			Target:  pushToMultistreamTargetInfo(pushInfo),
			Metrics: metrics,
		}
	}
	var stream *data.StreamMetrics
	if ss := metrics.stream; ss != nil {
		stream = &data.StreamMetrics{}
		// mediatime comes as -1 when not available
		if ss.MediaTimeMs >= 0 {
			stream.MediaTimeMs = &ss.MediaTimeMs
		}
	}
	return data.NewMediaServerMetricsEvent(nodeID, region, info.stream.ID, stream, multistream)
}

// streamMetrics aggregates all the data collected from Mist about a specific
// stream. Mist calls these values stats, but we use them as a single entry and
// will create analytics across multiple observations. So they are more like
// metrics for our infrastrucutre and that's what we call them from here on.
type streamMetrics struct {
	stream *mist.StreamStats
	pushes []*mist.Push
}

func compileStreamMetrics(mistStats *mist.MistStats) map[string]*streamMetrics {
	streamsMetrics := map[string]*streamMetrics{}
	getOrCreate := func(stream string) *streamMetrics {
		if metrics, ok := streamsMetrics[stream]; ok {
			return metrics
		}
		metrics := &streamMetrics{}
		streamsMetrics[stream] = metrics
		return metrics
	}

	for stream, stats := range mistStats.StreamsStats {
		getOrCreate(stream).stream = stats
	}
	for _, push := range mistStats.PushList {
		info := getOrCreate(push.Stream)
		info.pushes = append(info.pushes, push)
	}
	return streamsMetrics
}
