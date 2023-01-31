package mistapiconnector

import (
	"context"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/go-api-client"
	"github.com/livepeer/livepeer-data/pkg/data"
	"github.com/livepeer/livepeer-data/pkg/event"
	"github.com/livepeer/stream-tester/apis/mist"
	census "github.com/livepeer/stream-tester/internal/metrics"
	"golang.org/x/sync/errgroup"
)

const lastSeenBumpPeriod = 30 * time.Second

type infoProvider interface {
	getStreamInfo(mistID string) (*streamInfo, error)
}

type metricsCollector struct {
	nodeID, ownRegion string
	mapi              *mist.API
	lapi              *api.Client
	producer          *event.AMQPProducer
	amqpExchange      string
	infoProvider
}

func startMetricsCollector(ctx context.Context, period time.Duration, nodeID, ownRegion string, mapi *mist.API, lapi *api.Client, producer *event.AMQPProducer, amqpExchange string, infop infoProvider) {
	mc := &metricsCollector{nodeID, ownRegion, mapi, lapi, producer, amqpExchange, infop}
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

	eg := errGroupRecv{}
	eg.SetLimit(5)

	for streamID, metrics := range streamsMetrics {
		if err := ctx.Err(); err != nil {
			return err
		}

		streamID, metrics := streamID, metrics
		eg.GoRecovered(func() {
			info, err := c.getStreamInfo(streamID)
			if err != nil {
				glog.Errorf("Error getting stream info for streamId=%s err=%q", streamID, err)
				return
			}
			if info.isLazy {
				// avoid spamming metrics for playback-only catalyst instances. This means
				// that if mapic restarts we will stop sending metrics from previous
				// streams as well, but that's a minor issue (curr stream health is dying).
				glog.Infof("Skipping metrics for lazily created stream info. streamId=%q metrics=%+v", streamID, metrics)
				return
			}

			eg.GoRecovered(func() {
				info.mu.Lock()
				timeSinceBumped := time.Since(info.lastSeenBumpedAt)
				info.mu.Unlock()
				if timeSinceBumped <= lastSeenBumpPeriod {
					return
				}

				if _, err := c.lapi.SetActive(info.stream.ID, true, info.startedAt); err != nil {
					glog.Errorf("Error updating stream last seen. err=%q streamId=%q", err, info.stream.ID)
					return
				}

				info.mu.Lock()
				info.lastSeenBumpedAt = time.Now()
				info.mu.Unlock()
			})

			eg.GoRecovered(func() {
				mseEvent := createMetricsEvent(c.nodeID, c.ownRegion, info, metrics)
				err = c.producer.Publish(ctx, event.AMQPMessage{
					Exchange: c.amqpExchange,
					Key:      fmt.Sprintf("stream.metrics.%s", info.stream.ID),
					Body:     mseEvent,
				})
				if err != nil {
					glog.Errorf("Error sending mist stream metrics event. err=%q streamId=%q event=%+v", err, info.stream.ID, mseEvent)
				}
			})
		})
	}

	return eg.Wait()
}

func createMetricsEvent(nodeID, region string, info *streamInfo, metrics *streamMetrics) *data.MediaServerMetricsEvent {
	info.mu.Lock()
	defer info.mu.Unlock()
	multistream := make([]*data.MultistreamTargetMetrics, len(metrics.pushes))
	for i, push := range metrics.pushes {
		pushInfo := info.pushStatus[push.OriginalURI]
		if pushInfo == nil {
			glog.Infof("Mist exported metrics from unknown push. streamId=%q pushURL=%q", info.id, push.OriginalURI)
			continue
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

type errGroupRecv struct{ errgroup.Group }

func (eg *errGroupRecv) GoRecovered(f func()) {
	eg.Group.Go(func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				glog.Errorf("Panic in metrics collector. value=%v stack=%s", r, debug.Stack())
				err = fmt.Errorf("panic: %v", r)
			}
		}()
		f()
		return nil
	})
}
