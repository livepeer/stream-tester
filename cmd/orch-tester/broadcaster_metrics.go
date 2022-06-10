package main

import (
	"go.opencensus.io/stats/view"
	"sync"
)

type broadcasterMetrics struct {
	// Distribution metrics grouped by metric name
	lastSum   map[string]float64
	lastCount map[string]int
	rateSum   map[string]float64
	rateCount map[string]int

	// Error count metrics by grouped by error_code tag
	lastErrs map[string]int
	incErrs  map[string]int

	mu sync.Mutex
}

func (bm *broadcasterMetrics) reset() {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	bm.rateSum = map[string]float64{}
	bm.rateCount = map[string]int{}
	bm.incErrs = map[string]int{}
}

func (bm broadcasterMetrics) ExportView(viewData *view.Data) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	name := viewData.View.Name
	rows := viewData.Rows
	if len(rows) <= 0 {
		return
	}

	bm.handleDistributionMetrics(rows, name)
	bm.handleErrorCountMetrics(rows, name)
}

func (bm broadcasterMetrics) handleDistributionMetrics(rows []*view.Row, name string) {
	supportedMetrics := map[string]bool{
		"source_segment_duration_seconds":   true,
		"transcode_overall_latency_seconds": true,
		"upload_time_seconds":               true,
		"download_time_seconds":             true,
	}
	if !supportedMetrics[name] {
		return
	}

	// Sum metrics with different tags
	var sum float64
	var count int
	for _, r := range rows {
		d, ok := r.Data.(*view.DistributionData)
		if ok {
			sum += d.Sum()
			count += int(d.Count)
		}
	}

	// Get the rate between the last measurement
	rateSum := sum - bm.lastSum[name]
	rateCount := count - bm.lastCount[name]

	bm.rateSum[name] += rateSum
	bm.rateCount[name] += rateCount
	bm.lastSum[name] = sum
	bm.lastCount[name] = count
}

func (bm broadcasterMetrics) handleErrorCountMetrics(rows []*view.Row, name string) {
	supportedMetrics := map[string]bool{
		"segment_source_upload_failed_total": true,
		"discovery_errors_total":             true,
		"segment_transcode_failed_total":     true,
	}
	if !supportedMetrics[name] {
		return
	}

	for _, r := range rows {
		var errCode string
		for _, t := range r.Tags {
			if t.Key.Name() == "error_code" {
				errCode = t.Value
			}
		}
		if errCode == "" {
			return
		}

		d, ok := r.Data.(*view.CountData)
		if ok {
			inc := int(d.Value) - bm.lastErrs[errCode]
			bm.incErrs[errCode] += inc
			bm.lastErrs[errCode] = int(d.Value)
		}
	}
}

func (bm *broadcasterMetrics) avg(m string) float64 {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	if bm.rateCount[m] <= 0 {
		return 0
	}
	return bm.rateSum[m] / float64(bm.rateCount[m])
}

func (bm *broadcasterMetrics) incErrorCount() map[string]int {
	bm.mu.Lock()
	bm.mu.Unlock()

	return bm.incErrs
}
