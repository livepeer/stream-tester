package m3u8

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/livepeer/go-api-client"
	"github.com/livepeer/stream-tester/internal/metrics"
	"github.com/livepeer/stream-tester/internal/testers"
)

func InitCensus(service, version string) {
	hostname, _ := os.Hostname()
	metrics.InitCensus(hostname, version, service)
}

func Check(ctx context.Context, url string, profiles []api.Profile, expectedDuration time.Duration, timeout time.Duration) error {
	downloader := testers.NewM3utester2(ctx, url, false, false, false, false, timeout, nil, false)
	<-downloader.Done()
	stats := downloader.VODStats()
	if ok, ers := stats.IsOk(expectedDuration, false); !ok {
		return fmt.Errorf("playlist not ok: %s", ers)
	}
	if len(stats.SegmentsNum) != len(profiles)+1 {
		return fmt.Errorf("number of renditions doesn't match (has %d should %d)", len(stats.SegmentsNum), len(profiles)+1)
	}
	return nil
}