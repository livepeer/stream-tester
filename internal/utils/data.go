package utils

import (
	"math"
	"path"
	"sort"
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

// Add ..
func (lc *LatenciesCalculator) Add(data []time.Duration) {
	if len(data) > 0 {
		lc.latencies = append(lc.latencies, data)
	}
}

// Prepare ..
func (lc *LatenciesCalculator) Prepare() {
	// glog.Infof("=== lc len: %d", len(lc.latencies))
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
	// glog.Infof("== src: %v", lc.latencies)
	// glog.Infof("=== joined: %v", lc.joined)
	lc.latencies = nil
}

func getPercentile(values []time.Duration, percentile int) time.Duration {
	var per time.Duration
	var findex = float64(len(values)) * float64(percentile) / 100.0
	if math.Ceil(findex) == math.Floor(findex) {
		index := int(findex) - 1
		// glog.Infof("== getPercentile of %v findex %v index %d len %d", percentile, findex, index, len(values))
		per = (values[index] + values[index+1]) / 2
	} else {
		index := int(math.Round(findex)) - 1
		// glog.Infof("== getPercentile of %v findex %v index %d len %d", percentile, findex, index, len(values))
		per = values[index]
	}
	return per
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
	p5 = getPercentile(lc.joined, 50)
	p95 = getPercentile(lc.joined, 95)
	p99 = getPercentile(lc.joined, 99)
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
	m := mark / (time.Millisecond * 100)
	stp.m.Lock()
	stp.times[m] = t
	stp.m.Unlock()
}

// GetTime returns time for mark
func (stp *SyncedTimesMap) GetTime(mark time.Duration, name string) (time.Time, bool) {
	// glog.Infof("=== get for name %s", name)
	_, file := path.Split(name)
	m := mark / (time.Millisecond * 100)
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
