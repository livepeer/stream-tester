package vodtester

import (
	"context"
	"time"

	"github.com/golang/glog"
	api "github.com/livepeer/go-api-client"
)

type (
	// IVodTester ...
	IVodTester interface {
		// Start start test. Blocks until finished.
		Start(fileName string, testDuration, pauseDuration time.Duration) (int, error)
		Cancel()
		Done() <-chan struct{}
		Clean()
	}

	VodTesterOptions struct {
		API *api.Client
	}

	vodTester struct {
		ctx    context.Context
		cancel context.CancelFunc
		lapi   *api.Client
	}
)

// NewVodTester ...
func NewVodTester(gctx context.Context, opts VodTesterOptions) IVodTester {
	ctx, cancel := context.WithCancel(gctx)
	vt := &vodTester{
		lapi:   opts.API,
		ctx:    ctx,
		cancel: cancel,
	}
	return vt
}

func (vt *vodTester) Start(fileUrl string, testDuration, pauseDuration time.Duration) (int, error) {
	defer vt.cancel()
	var (
		err       error
		startTime = time.Now()
	)

	assetName := "vod-test-asset"
	importAsset, importTask, err := vt.lapi.ImportAsset(fileUrl, assetName)
	if err != nil {
		glog.Errorf("Error creating import task err=%v", err)
		return 242, err
	}
	glog.Infof("Importing asset taskId=%s outputAssetId=%s", importTask.ID, importAsset.ID)

	startTime = time.Now()
	for {
		glog.Infof("Waiting %s for import assetId=%s, elapsed=%s", pauseDuration, importAsset.ID, time.Since(startTime))
		time.Sleep(pauseDuration)

		if err = vt.isCancelled(); err != nil {
			return 0, err
		}

		asset, err := vt.lapi.GetAsset(importAsset.ID)
		if err != nil {
			glog.Errorf("Error retrieving asset id=%s err=%v", importAsset.ID, err)
			return 243, err
		}
		if asset.Status.Phase == "ready" {
			break
		}
		if asset.Status.Phase != "waiting" {
			glog.Errorf("Error importing asset assetId=%s, task id=%s err=%v", importAsset.ID, importTask.ID, err)
			return 244, err
		}
	}

	transcodeAsset, transcodeTask, err := vt.lapi.TranscodeAsset(importAsset.ID, assetName, api.StandardProfiles[0])
	if err != nil {
		glog.Errorf("Error creating transcode task err=%v", err)
		return 242, err
	}
	glog.Infof("Asset imported id=%s, transcoding taskId=%s outputAssetId=%s", importAsset.ID, transcodeTask.ID, transcodeAsset.ID)

	startTime = time.Now()
	for {
		glog.Infof("Waiting %s for transcode assetId=%s outputAssetId=%s, elapsed=%s", pauseDuration, importAsset.ID, transcodeAsset.ID, time.Since(startTime))
		time.Sleep(pauseDuration)

		if err = vt.isCancelled(); err != nil {
			return 0, err
		}

		asset, err := vt.lapi.GetAsset(transcodeAsset.ID)
		if err != nil {
			glog.Errorf("Error retrieving asset id=%s err=%v", transcodeAsset.ID, err)
			return 243, err
		}
		if asset.Status.Phase == "ready" {
			break
		}
		if asset.Status.Phase != "waiting" {
			glog.Errorf("Error transcoding asset assetId=%s, task id=%s outputAssetId=%s err=%v", importAsset.ID, transcodeTask.ID, transcodeAsset.ID, err)
			return 244, err
		}
	}

	exportTask, err := vt.lapi.ExportAsset(importAsset.ID)
	if err != nil {
		glog.Errorf("Error creating export task err=%v", err)
		return 245, err
	}
	glog.Infof("Transcode complete assetId=%s ready, exporting assetId=%s, taskId=%s", transcodeAsset.ID, importAsset.ID, exportTask.ID)

	startTime = time.Now()
	for {
		glog.Infof("Waiting %s for asset id=%s to be exported, elapsed=%s", pauseDuration, transcodeAsset.ID, time.Since(startTime))
		time.Sleep(pauseDuration)
		if err = vt.isCancelled(); err != nil {
			return 0, err
		}

		task, err := vt.lapi.GetTask(exportTask.ID)
		if err != nil {
			glog.Errorf("Error retrieving task id=%s err=%v", exportTask.ID, err)
			return 246, err
		}
		if task.Output.Export.IPFS.VideoFileGatewayUrl != "" {
			glog.Infof("Export success, task id=%s assetId=%s ipfs link=%s", exportTask.ID, exportTask.InputAssetID, task.Output.Export.IPFS.VideoFileGatewayUrl)
			break
		}
	}

	glog.Info("Done VOD Test")

	return 0, err
}

func (vt *vodTester) isCancelled() error {
	select {
	case <-vt.ctx.Done():
		return context.Canceled
	default:
	}
	return nil
}

func (vt *vodTester) Cancel() {
	vt.cancel()
}

func (vt *vodTester) Done() <-chan struct{} {
	return vt.ctx.Done()
}

func (rt *vodTester) Clean() {
	// TODO
}
