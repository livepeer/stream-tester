package utils

import (
	"math"
	"path"
	"sort"
	"strings"
	"sync"
	"time"
)

type durations []time.Duration

func (dur durations) Len() int           { return len(dur) }
func (dur durations) Swap(i, j int)      { dur[i], dur[j] = dur[j], dur[i] }
func (dur durations) Less(i, j int) bool { return dur[i] < dur[j] }

// LatenciesCalculator calculates average and median latencies
type LatenciesCalculator struct {
	latencies [][]time.Duration
	joined    []time.Duration
}

// DurationsCapped ...
type DurationsCapped struct {
	cap       int
	durations []time.Duration
	fv        []float64
}

// NewDurations ...
func NewDurations(cap int) *DurationsCapped {
	return &DurationsCapped{
		cap: cap,
	}
}

// Add ...
func (ds *DurationsCapped) Add(d time.Duration) {
	ds.durations = append(ds.durations, d)
	if len(ds.durations) > ds.cap {
		ds.durations = ds.durations[1:]
	}
}

// AddFloat ...
func (ds *DurationsCapped) AddFloat(v float64) {
	ds.fv = append(ds.fv, v)
	if len(ds.fv) > ds.cap {
		ds.fv = ds.fv[1:]
	}
}

// GetPercentile ...
func (ds *DurationsCapped) GetPercentile(percentile ...int) []time.Duration {
	if len(ds.durations) < 8 {
		return make([]time.Duration, len(percentile))
	}
	cp := make([]time.Duration, len(ds.durations))
	copy(cp, ds.durations)
	dur := durations(cp)
	sort.Sort(dur)
	res := make([]time.Duration, 0, len(percentile))
	for _, p := range percentile {
		res = append(res, GetPercentile(cp, p))
	}
	return res
}

// GetPercentileFloat ...
func (ds *DurationsCapped) GetPercentileFloat(percentile ...int) []float64 {
	if len(ds.fv) < 8 {
		return make([]float64, len(percentile))
	}
	cp := make([]float64, len(ds.fv))
	copy(cp, ds.fv)
	sort.Float64s(cp)
	res := make([]float64, 0, len(percentile))
	for _, p := range percentile {
		res = append(res, getPercentileFloat(cp, p))
	}
	return res
}

// Calc return average, 50th, 95th 99th percentiles
func (ds *DurationsCapped) Calc() (time.Duration, time.Duration, time.Duration, time.Duration) {
	var avg, p5, p95, p99 time.Duration
	if len(ds.durations) == 0 {
		return avg, p5, p95, p99
	}
	dur := durations(ds.durations)
	sort.Sort(dur)
	for _, v := range ds.durations {
		avg += v
	}
	avg /= time.Duration(len(ds.durations))
	p5 = GetPercentile(ds.durations, 50)
	p95 = GetPercentile(ds.durations, 95)
	p99 = GetPercentile(ds.durations, 99)
	return avg, p5, p95, p99
}

func getPercentileFloat(values []float64, percentile int) float64 {
	var per float64
	var findex = float64(len(values)) * float64(percentile) / 100.0
	if math.Ceil(findex) == math.Floor(findex) {
		index := int(findex) - 1
		per = (values[index] + values[index+1]) / 2
	} else {
		index := int(math.Round(findex)) - 1
		per = values[index]
	}
	return per
}

// Add ..
func (lc *LatenciesCalculator) Add(data []time.Duration) {
	if len(data) > 0 {
		lc.latencies = append(lc.latencies, data)
	}
}

// Prepare ..
func (lc *LatenciesCalculator) Prepare() {
	if len(lc.latencies) == 0 {
		return
	}
	var total int
	for _, l := range lc.latencies {
		total += len(l) - 1
	}
	lc.joined = make([]time.Duration, total, total)
	i := 0
	for _, l := range lc.latencies {
		copy(lc.joined[i:], l[1:]) // first segments latency is non-representative because of long start up time
		i += len(l) - 1
	}
	lc.latencies = nil
}

// GetPercentile calc percentail for duration
func GetPercentile(values []time.Duration, percentile int) time.Duration {
	var per time.Duration
	var findex = float64(len(values)) * float64(percentile) / 100.0
	if math.Ceil(findex) == math.Floor(findex) {
		index := int(findex) - 1
		per = (values[index] + values[index+1]) / 2
	} else {
		index := int(math.Round(findex)) - 1
		per = values[index]
	}
	return per
}

// Raw returns joined latencies array
func (lc *LatenciesCalculator) Raw() []time.Duration {
	return lc.joined
}

// Calc return average, 50th, 95th 99th percentiles
func (lc *LatenciesCalculator) Calc() (time.Duration, time.Duration, time.Duration, time.Duration) {
	var avg, p5, p95, p99 time.Duration
	if len(lc.joined) == 0 {
		return avg, p5, p95, p99
	}
	dur := durations(lc.joined)
	sort.Sort(dur)
	for _, v := range lc.joined {
		avg += v
	}
	avg /= time.Duration(len(lc.joined))
	p5 = GetPercentile(lc.joined, 50)
	p95 = GetPercentile(lc.joined, 95)
	p99 = GetPercentile(lc.joined, 99)
	return avg, p5, p95, p99
}

// SyncedTimesMap ...
type SyncedTimesMap struct {
	times  map[time.Duration]time.Time
	byName map[string]time.Time
	m      sync.RWMutex
}

// NewSyncedTimesMap rturns new SyncedTimesMap
func NewSyncedTimesMap() *SyncedTimesMap {
	return &SyncedTimesMap{
		times:  make(map[time.Duration]time.Time),
		byName: make(map[string]time.Time),
	}
}

// SetTime sets time
func (stp *SyncedTimesMap) SetTime(mark time.Duration, t time.Time) {
	m := mark / (time.Millisecond * 500)
	stp.m.Lock()
	stp.times[m] = t
	stp.m.Unlock()
}

// GetTime returns time for mark
func (stp *SyncedTimesMap) GetTime(mark time.Duration, name string) (time.Time, bool) {
	_, file := path.Split(name)
	m := mark / (time.Millisecond * 500)
	// stp.m.RLock()
	stp.m.Lock()
	t, h := stp.times[m]
	if h {
		if _, hn := stp.byName[file]; !hn {
			// stp.m.Lock()
			stp.byName[file] = t
			// stp.m.Unlock()
		}
	} else {
		t, h = stp.byName[file]
	}
	// stp.m.RUnlock()
	stp.m.Unlock()
	return t, h
}

// StringsSliceContains ...
func StringsSliceContains(ss []string, st string) bool {
	for _, s := range ss {
		if s == st {
			return true
		}
	}
	return false
}

var cleanReplacer = strings.NewReplacer(`/`, `_`, `\`, `_`, `?`, `_`, `=`, `_`, `+`, `_`)

// CleanFileName removes special symbols from string
func CleanFileName(s string) string {
	return cleanReplacer.Replace(s)
}
