package common

import (
	"context"
	"fmt"
	"github.com/golang/glog"
	"github.com/livepeer/go-api-client"
	"time"
)

type (
	TesterApp struct {
		Ctx                      context.Context
		CancelFunc               context.CancelFunc
		CatalystPipelineStrategy string
		Lapi                     *api.Client
	}

	TesterOptions struct {
		API                      *api.Client
		CatalystPipelineStrategy string
	}
)

func (ta *TesterApp) WaitTaskProcessing(taskPollDuration time.Duration, processingTask api.Task) (*api.Task, error) {
	startTime := time.Now()
	for {
		time.Sleep(taskPollDuration)

		if err := ta.isCancelled(); err != nil {
			return nil, err
		}

		// we already sleep before the first check, so no need for strong consistency
		task, err := ta.Lapi.GetTask(processingTask.ID, false)
		if err != nil {
			glog.Errorf("Error retrieving task id=%s err=%v", processingTask.ID, err)
			return nil, fmt.Errorf("error retrieving task id=%s: %w", processingTask.ID, err)
		}
		if task.Status.Phase == "completed" {
			glog.Infof("Task success, taskId=%s", task.ID)
			return task, nil
		}
		if task.Status.Phase != "pending" && task.Status.Phase != "running" && task.Status.Phase != "waiting" {
			glog.Errorf("Error processing task, taskId=%s status=%s error=%v", task.ID, task.Status.Phase, task.Status.ErrorMessage)
			return task, fmt.Errorf("error processing task, taskId=%s status=%s error=%v", task.ID, task.Status.Phase, task.Status.ErrorMessage)
		}

		glog.Infof("Waiting for task to be processed id=%s pollWait=%s elapsed=%s progressPct=%.1f%%", task.ID, taskPollDuration, time.Since(startTime), 100*task.Status.Progress)
	}
}

func (ta *TesterApp) isCancelled() error {
	select {
	case <-ta.Ctx.Done():
		return context.Canceled
	default:
	}
	return nil
}

func (ta *TesterApp) Cancel() {
	ta.CancelFunc()
}

func (ta *TesterApp) Done() <-chan struct{} {
	return ta.Ctx.Done()
}
