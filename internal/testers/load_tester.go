package testers

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/stream-tester/model"
)

type (
	// loadTester general load tester
	loadTester struct {
		finite
		streamStarter       model.StreamStarter
		streams             map[int]model.OneTestStream
		id                  int
		delayBetweenStreams time.Duration
		mu                  sync.Mutex
	}
)

// NewLoadTester creates new loadTester object
func NewLoadTester(pctx context.Context, streamStarter model.StreamStarter, delayBetweenStreams time.Duration) model.ILoadTester {
	ctx, cancel := context.WithCancel(pctx)
	lt := &loadTester{
		finite: finite{
			ctx:    ctx,
			cancel: cancel,
		},
		streamStarter:       streamStarter,
		streams:             make(map[int]model.OneTestStream),
		delayBetweenStreams: delayBetweenStreams,
	}
	return lt
}

func (lt *loadTester) Start(sourceFileName string, waitForTarget, oneStreamTime, overallTestTime time.Duration, sim int) error {
	if lt.Finished() {
		panic("shouldn't be reused")
	}
	if sim <= 0 {
		panic("Should start more than zero streams")
	}
	started := time.Now()
	glog.Infof("Starting first stream")
	stream, err := lt.streamStarter(lt.ctx, sourceFileName, waitForTarget, oneStreamTime)
	if err != nil {
		glog.Errorf("Error starting stream: %v", err)
		return err
	}
	lt.streams[lt.id] = stream
	lt.id++
	glog.Infof("Starting %d streams", sim-1)
	lastStatsPrint := time.Now()
	for {
		if lt.Finished() {
			break
		}
		if len(lt.streams) < sim {
			if lt.delayBetweenStreams > 0 {
				delay := lt.delayBetweenStreams + time.Duration(rand.Intn(int(lt.delayBetweenStreams.Milliseconds())))*time.Millisecond
				glog.V(model.VERBOSE).Infof("Waiting for %s before starting stream %d", delay, lt.id)
				time.Sleep(delay)
			}
			stream, err := lt.streamStarter(lt.ctx, sourceFileName, waitForTarget, oneStreamTime)
			if err != nil {
				glog.Errorf("Error starting stream: %v", err)
			} else {
				lt.streams[lt.id] = stream
				lt.id++
			}
		}
		// print stats
		if time.Since(lastStatsPrint) > 30*time.Second {
			lt.printStats()
			lastStatsPrint = time.Now()
		}
		// check if stream is finished and needs to be removed
		for id, stream := range lt.streams {
			if stream.Finished() {
				delete(lt.streams, id)
			}
		}
		if time.Since(started) > overallTestTime {
			break
		}
		delay := 10 * time.Second
		if len(lt.streams) < sim {
			delay = 100 * time.Millisecond
		}
		time.Sleep(delay)
	}
	glog.Infof("Load test finished after %s", time.Since(started))
	lt.printStats()
	lt.Cancel()

	return nil
}

func (lt *loadTester) printStats() {
	stats, err := lt.Stats()
	if err != nil {
		glog.Errorf("Error getting stats: %v", err)
		return
	}
	msg := fmt.Sprintf("\n%s\n", stats.FormatForConsole())
	glog.V(model.SHORT).Info(msg)
}

func (lt *loadTester) Stats() (model.StatsMany, error) {
	lt.mu.Lock()
	var stats model.StatsMany
	stats.ActiveStreams = len(lt.streams)
	var num float64
	for _, stream := range lt.streams {
		stats1, err := stream.Stats()
		if err == nil {
			stats.SuccessRate += stats1.SuccessRate
			num++
		}
	}
	if num > 0 {
		stats.SuccessRate = stats.SuccessRate / num
	}
	lt.mu.Unlock()
	stats.Finished = lt.Finished()
	return stats, nil
}
