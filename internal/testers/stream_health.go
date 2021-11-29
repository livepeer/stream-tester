package testers

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/livepeer-data/pkg/client"
)

type (
	// AnalyzerByRegion is a map of regions (generally just the base URL) to an
	// analyzer client configured to connect there.
	AnalyzerByRegion map[string]client.Analyzer

	streamHealth struct {
		finite
		streamID string
		clients  AnalyzerByRegion
	}
)

func NewStreamHealth(parent context.Context, streamID string, clients AnalyzerByRegion, waitForTarget time.Duration) Finite {
	ctx, cancel := context.WithCancel(parent)
	sh := &streamHealth{
		finite: finite{
			ctx:    ctx,
			cancel: cancel,
		},
		streamID: streamID,
		clients:  clients,
	}
	go sh.workerLoop(waitForTarget)
	return sh
}

func (h *streamHealth) workerLoop(waitForTarget time.Duration) {
	defer h.cancel()
	unhealthyTimeout := time.NewTimer(waitForTarget)
	checkTicker := time.NewTicker(5 * time.Second)
	defer checkTicker.Stop()
	var unhealthyRegions []checkResult
	for {
		select {
		case <-h.ctx.Done():
			return
		case <-unhealthyTimeout.C:
			var regionErrs []string
			for _, check := range unhealthyRegions {
				regionErrs = append(regionErrs, fmt.Sprintf("%s: %s", check.region, check.err))
			}
			err := fmt.Errorf("stream failed to become healthy: %s", strings.Join(regionErrs, "; "))
			glog.Errorf("Stream failed to become healthy after timeout=%v: %v", waitForTarget, err)
			h.fatalEnd(err)
			return
		case <-checkTicker.C:
			results := h.checkAllRegions()
			unhealthyRegions = unhealthyRegions[:0]
			for res := range results {
				if res.err != nil {
					unhealthyRegions = append(unhealthyRegions, res)
				}
			}
			if len(unhealthyRegions) == 0 {
				unhealthyTimeout.Stop()
			} else if unhealthyTimeout == nil {
				unhealthyTimeout.Reset(waitForTarget)
			}
		}
	}
}

type checkResult struct {
	region string
	err    error
}

func (h *streamHealth) checkAllRegions() <-chan checkResult {
	results := make(chan checkResult, 2)
	wg := sync.WaitGroup{}
	for region := range h.clients {
		wg.Add(1)
		go func(region string) {
			defer wg.Done()
			health, err := h.clients[region].GetStreamHealth(h.ctx, h.streamID)
			if err != nil {
				glog.Warningf("Stream health error on region=%q, err=%q", region, err)
				err = fmt.Errorf("error fetching stream health: %w", err)
			} else if healthy := health.Healthy.Status; healthy == nil || !*healthy {
				glog.Warningf("Stream unhealthy on region=%q, health=%+v", region, health)
				err = fmt.Errorf("stream is unhealthy")
			} else if healthy != nil && time.Since(health.Healthy.LastProbeTime.Time) > time.Minute {
				glog.Warningf("Stream health outdated on region=%q, health=%+v", region, health)
				err = fmt.Errorf("stream health is outdated (%v)", health.Healthy.LastProbeTime.Time)
			}
			results <- checkResult{region, err}
		}(region)
	}
	go func() {
		wg.Wait()
		close(results)
	}()
	return results
}
