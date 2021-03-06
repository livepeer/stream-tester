package recordtester

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/stream-tester/apis/livepeer"
	"github.com/livepeer/stream-tester/messenger"
)

type (
	// IContinuousRecordTester ...
	IContinuousRecordTester interface {
		// Start start test. Blocks until error.
		Start(fileName string, testDuration, pauseDuration, pauseBetweenTests time.Duration) error
		Cancel()
		Done() <-chan struct{}
	}

	continuousRecordTester struct {
		lapi   *livepeer.API
		ctx    context.Context
		cancel context.CancelFunc
		host   string // API host being tested
	}
)

// NewContinuousRecordTester returns new object
func NewContinuousRecordTester(gctx context.Context, lapi *livepeer.API) IContinuousRecordTester {
	ctx, cancel := context.WithCancel(gctx)
	server := lapi.GetServer()
	u, _ := url.Parse(server)
	crt := &continuousRecordTester{
		lapi:   lapi,
		ctx:    ctx,
		cancel: cancel,
		host:   u.Host,
	}
	return crt
}

func (crt *continuousRecordTester) Start(fileName string, testDuration, pauseDuration, pauseBetweenTests time.Duration) error {
	glog.Infof("Starting continuous test of %s", crt.host)
	for {
		msg := fmt.Sprintf(":arrow_right: Starting %s recordings test stream to %s", 2*testDuration, crt.host)
		glog.Info(msg)
		messenger.SendMessage(msg)
		rt := NewRecordTester(crt.ctx, crt.lapi, true)
		es, err := rt.Start(fileName, testDuration, pauseDuration)
		if err == context.Canceled {
			msg := fmt.Sprintf("Test of %s cancelled", crt.host)
			messenger.SendMessage(msg)
			return err
		} else if err != nil || es != 0 {
			msg := fmt.Sprintf(":rotating_light: Test of %s ended with err=%v errCode=%v", crt.host, err, es)
			messenger.SendFatalMessage(msg)
			glog.Warning(msg)
		} else {
			msg := fmt.Sprintf(":white_check_mark: Test of %s succeed", crt.host)
			messenger.SendMessage(msg)
			glog.Warning(msg)
		}
		glog.Infof("Waiting %s before next test", pauseBetweenTests)
		time.Sleep(pauseBetweenTests)
		select {
		case <-crt.ctx.Done():
			return context.Canceled
		default:
		}
	}
}

func (crt *continuousRecordTester) Cancel() {
	crt.cancel()
}

func (crt *continuousRecordTester) Done() <-chan struct{} {
	return crt.ctx.Done()
}
