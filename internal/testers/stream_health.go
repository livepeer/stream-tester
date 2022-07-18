package testers

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/livepeer-data/health/reducers"
	"github.com/livepeer/livepeer-data/pkg/client"
	"github.com/livepeer/livepeer-data/pkg/data"
	"github.com/livepeer/stream-tester/model"
)

type (
	// AnalyzerByRegion is a map of regions (generally just the base URL) to an
	// analyzer client configured to connect there.
	AnalyzerByRegion map[string]client.Analyzer

	StreamHealthError struct {
		Message string
	}

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
	unhealthyTimeout := time.After(waitForTarget)
	checkTicker := time.NewTicker(5 * time.Second)
	logErrs := false
	time.AfterFunc(waitForTarget/2, func() { logErrs = true })

	defer checkTicker.Stop()
	var unhealthyRegions []checkResult
	for {
		select {
		case <-h.ctx.Done():
			return
		case <-checkTicker.C:
			results := h.checkAllRegions(logErrs)
			unhealthyRegions = unhealthyRegions[:0]
			for res := range results {
				if res.err != nil {
					unhealthyRegions = append(unhealthyRegions, res)
				}
			}
			if len(unhealthyRegions) == 0 {
				unhealthyTimeout = nil
			} else if unhealthyTimeout == nil {
				unhealthyTimeout = time.After(waitForTarget)
			}
		case <-unhealthyTimeout:
			errsRegions := map[string][]string{}
			for _, check := range unhealthyRegions {
				err := check.err.Error()
				errsRegions[err] = append(errsRegions[err], fmt.Sprintf("`%s`", check.region))
			}
			aggErrs := make([]string, 0, len(errsRegions))
			for err, regions := range errsRegions {
				regionsStr := "`all` regions"
				if len(regions) < len(h.clients) {
					regionsStr = "[" + strings.Join(regions, ", ") + "]"
				}
				sort.Slice(regions, func(i, j int) bool { return regions[i] < regions[j] })
				aggErrs = append(aggErrs, fmt.Sprintf("%s in %s", err, regionsStr))
			}
			sort.Slice(aggErrs, func(i, j int) bool { return aggErrs[i] < aggErrs[j] })

			msg := fmt.Sprintf("Global Stream Health API: stream did not become healthy on global analyzers after `%s`: %s",
				waitForTarget, strings.Join(aggErrs, "; "))
			h.fatalEnd(StreamHealthError{msg})
			return
		}
	}
}

type checkResult struct {
	region string
	err    error
}

func (h *streamHealth) checkAllRegions(logErrs bool) <-chan checkResult {
	results := make(chan checkResult, 2)
	wg := sync.WaitGroup{}
	startTime := time.Now()
	for region := range h.clients {
		wg.Add(1)
		go func(region string) {
			defer wg.Done()
			glog.V(model.INSANE).Infof("Checking stream health for region=%s", region)
			health, err := h.clients[region].GetStreamHealth(h.ctx, h.streamID)
			if err == nil {
				conds := strings.Join(conditionsStatus(health), ", ")
				if healthy := health.Healthy.Status; healthy == nil {
					err = fmt.Errorf("stream health info is unavailable (`nil`). health conditions: %s", conds)
				} else if !*healthy {
					err = fmt.Errorf("stream is unhealthy. health conditions: %s", conds)
				} else if age := startTime.Sub(health.Healthy.LastProbeTime.Time); age > time.Minute {
					err = fmt.Errorf("stream health is outdated (`%s`)", age)
				}
			}
			if err != nil && (logErrs || bool(glog.V(model.VVERBOSE))) {
				rawHealth, jsonErr := json.Marshal(health)
				if jsonErr != nil {
					rawHealth = []byte(fmt.Sprintf("%+v", health))
				}
				glog.Warningf("Stream not healthy on region=%q, err=%q, health=%s", region, err, rawHealth)
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

func conditionsStatus(health *data.HealthStatus) []string {
	failed := []string{}
	for _, cond := range health.Conditions {
		if cond.Status != nil {
			failed = append(failed, fmt.Sprintf("`%s=%v`", cond.Type, *cond.Status))
		} else if cond.Type != reducers.ConditionMultistreaming {
			failed = append(failed, fmt.Sprintf("`%s=nil` (unavailable)", cond.Type))
		}
	}
	return failed
}

func (e StreamHealthError) Error() string {
	return e.Message
}
