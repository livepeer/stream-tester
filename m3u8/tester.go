package m3u8

import (
	"context"
	"fmt"
	"time"

	"github.com/livepeer/go-api-client"
	"github.com/livepeer/stream-tester/internal/testers"
)

func Check(ctx context.Context, url string, expectedDuration time.Duration) error {
	downloader := testers.NewM3utester2(ctx, url, false, false, false, false, 5*time.Second, nil, false)
	<-downloader.Done()
	stats := downloader.VODStats()
	if len(stats.SegmentsNum) != len(api.StandardProfiles)+1 {
		return fmt.Errorf("number of renditions doesn't match (has %d should %d)", len(stats.SegmentsNum), len(api.StandardProfiles)+1)
	}
	if ok, ers := stats.IsOk(expectedDuration, false); !ok {
		return fmt.Errorf("playlist not ok: %s", ers)
	}
	return nil
}
