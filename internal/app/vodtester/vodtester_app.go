package vodtester

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/golang/glog"
	api "github.com/livepeer/go-api-client"
)

type (
	// IVodTester ...
	IVodTester interface {
		// Start start test. Blocks until finished.
		Start(fileName string, taskPollDuration time.Duration) (int, error)
		Cancel()
		Done() <-chan struct{}
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

func (vt *vodTester) Start(fileUrl string, taskPollDuration time.Duration) (int, error) {
	defer vt.cancel()
	var (
		err       error
		startTime = time.Now()
	)

	hostName, _ := os.Hostname()
	assetName := fmt.Sprintf("vod_test_asset_%s_%s", hostName, time.Now().Format("2006-01-02T15:04:05Z07:00"))
	importAsset, importTask, err := vt.lapi.ImportAsset(fileUrl, assetName)
	if err != nil {
		glog.Errorf("Error importing asset err=%v", err)
		return 242, fmt.Errorf("error importing asset: %w", err)
	}
	glog.Infof("Importing asset taskId=%s outputAssetId=%s", importTask.ID, importAsset.ID)

	startTime = time.Now()
	for {
		glog.Infof("Waiting %s for import assetId=%s, elapsed=%s", taskPollDuration, importAsset.ID, time.Since(startTime))
		time.Sleep(taskPollDuration)

		if err = vt.isCancelled(); err != nil {
			return 0, err
		}

		asset, err := vt.lapi.GetAsset(importAsset.ID)
		if err != nil {
			glog.Errorf("Error retrieving asset id=%s err=%v", importAsset.ID, err)
			return 243, fmt.Errorf("error retrieving asset id=%s: %w", importAsset.ID, err)
		}
		if asset.Status.Phase == "ready" {
			break
		}
		if asset.Status.Phase != "waiting" {
			glog.Errorf("Error importing asset assetId=%s, taskId=%s, status=%s, err=%v", asset.ID, importTask.ID, asset.Status.Phase, asset.Status.ErrorMessage)
			return 244, fmt.Errorf("error importing asset assetId=%s, taskId=%s, status=%s, err=%v", asset.ID, importTask.ID, asset.Status.Phase, asset.Status.ErrorMessage)
		}
	}

	transcodeAsset, transcodeTask, err := vt.lapi.TranscodeAsset(importAsset.ID, assetName, api.StandardProfiles[0])
	if err != nil {
		glog.Errorf("Error transcoding asset id=%s, err=%v", importAsset.ID, err)
		return 242, fmt.Errorf("error transcoding asset id=%s: %w", importAsset.ID, err)
	}
	glog.Infof("Asset imported id=%s, transcoding taskId=%s outputAssetId=%s", importAsset.ID, transcodeTask.ID, transcodeAsset.ID)

	startTime = time.Now()
	for {
		glog.Infof("Waiting %s for transcode assetId=%s outputAssetId=%s, elapsed=%s", taskPollDuration, importAsset.ID, transcodeAsset.ID, time.Since(startTime))
		time.Sleep(taskPollDuration)

		if err = vt.isCancelled(); err != nil {
			return 0, err
		}

		asset, err := vt.lapi.GetAsset(transcodeAsset.ID)
		if err != nil {
			glog.Errorf("Error retrieving asset id=%s err=%v", transcodeAsset.ID, err)
			return 243, fmt.Errorf("error retrieving asset id=%s: %w", transcodeAsset.ID, err)
		}
		if asset.Status.Phase == "ready" {
			break
		}
		if asset.Status.Phase != "waiting" {
			glog.Errorf("Error transcoding asset assetId=%s, taskId=%s, outputAssetId=%s, status=%s, err=%v", importAsset.ID, transcodeTask.ID, asset.ID, asset.Status.Phase, asset.Status.ErrorMessage)
			return 244, fmt.Errorf("error transcoding asset assetId=%s, taskId=%s, outputAssetId=%s, status=%s, err=%v", importAsset.ID, transcodeTask.ID, asset.ID, asset.Status.Phase, asset.Status.ErrorMessage)
		}
	}

	exportTask, err := vt.lapi.ExportAsset(importAsset.ID)
	if err != nil {
		glog.Errorf("Error exporting asset id=%s err=%v", importAsset.ID, err)
		return 245, fmt.Errorf("error exporting asset id=%s: %w", importAsset.ID, err)
	}
	glog.Infof("Transcode complete assetId=%s ready, exporting assetId=%s, taskId=%s", transcodeAsset.ID, importAsset.ID, exportTask.ID)

	startTime = time.Now()
	for {
		glog.Infof("Waiting %s for asset id=%s to be exported, elapsed=%s", taskPollDuration, transcodeAsset.ID, time.Since(startTime))
		time.Sleep(taskPollDuration)
		if err = vt.isCancelled(); err != nil {
			return 0, err
		}

		task, err := vt.lapi.GetTask(exportTask.ID)
		if err != nil {
			glog.Errorf("Error retrieving task id=%s err=%v", exportTask.ID, err)
			return 246, fmt.Errorf("error retrieving task id=%s: %w", exportTask.ID, err)
		}
		if task.Status.Phase == "completed" {
			if task.Output != nil && task.Output.Export != nil && task.Output.Export.IPFS != nil && task.Output.Export.IPFS.VideoFileGatewayUrl != "" {
				glog.Infof("Export success, taskId=%s assetId=%s ipfsLink=%s", task.ID, task.InputAssetID, task.Output.Export.IPFS.VideoFileGatewayUrl)
				break
			}
			glog.Errorf("Error exporting asset, completed without ipfsLink taskId=%s assetId=%s", task.ID, task.InputAssetID)
			return 247, fmt.Errorf("error exporting asset, completed without ipfsLink, taskId=%s assetId=%s", task.ID, task.InputAssetID)
		}
		if task.Status.Phase != "pending" && task.Status.Phase != "running" && task.Status.Phase != "waiting" {
			glog.Errorf("Error exporting asset, taskId=%s assetId=%s status=%s error=%v", task.ID, task.InputAssetID, task.Status.Phase, task.Status.ErrorMessage)
			return 248, fmt.Errorf("error exporting asset, taskId=%s assetId=%s status=%s error=%v", task.ID, task.InputAssetID, task.Status.Phase, task.Status.ErrorMessage)
		}
	}

	filePath := "/tmp/uploadtester.mp4"
	resp, err := http.Get(fileUrl)

	if err != nil {
		glog.Errorf("Error downloading fileUrl=%s err=%v", fileUrl, err)
	}
	defer resp.Body.Close()

	out, err := os.Create(filePath)
	if err != nil {
		glog.Errorf("Error creating file filePath=%s err=%v", filePath, err)
	}
	defer out.Close()

	assetName = fmt.Sprintf("vod_test_asset_%s_%s", hostName, time.Now().Format("2006-01-02T15:04:05Z07:00"))
	requestUpload, err := vt.lapi.RequestUpload(assetName)

	if err != nil {
		glog.Errorf("Error requesting upload for assetName=%s err=%v", assetName, err)
		return 249, fmt.Errorf("error requesting upload for assetName=%s: %w", assetName, err)
	}

	uploadEndpoint := requestUpload.Url
	uploadAsset := requestUpload.Asset
	uploadTask := api.Task{
		ID: requestUpload.Task.ID,
	}

	glog.Infof("Uploading to endpoint=%s", uploadEndpoint)

	err = vt.lapi.UploadAsset(uploadEndpoint, out)
	if err != nil {
		glog.Errorf("Error uploading file filePath=%s err=%v", filePath, err)
		return 250, fmt.Errorf("error uploading for assetId=%s taskId=%s: %w", uploadAsset.ID, uploadTask.ID, err)
	}

	err = vt.checkAssetProcessing(taskPollDuration, uploadAsset, uploadTask)

	if err != nil {
		glog.Errorf("Error processing asset assetId=%s taskId=%s", uploadAsset.ID, uploadTask.ID)
		return 251, err
	}

	assetName = fmt.Sprintf("vod_test_asset_%s_%s", hostName, time.Now().Format("2006-01-02T15:04:05Z07:00"))
	requestUpload, err = vt.lapi.RequestUpload(assetName)

	if err != nil {
		glog.Errorf("Error requesting upload for assetName=%s err=%v", assetName, err)
		return 252, fmt.Errorf("error requesting upload for assetName=%s: %w", assetName, err)
	}

	tusUploadEndpoint := requestUpload.TusEndpoint
	uploadAsset = requestUpload.Asset
	uploadTask = api.Task{
		ID: requestUpload.Task.ID,
	}

	err = vt.lapi.ResumableUpload(tusUploadEndpoint, filePath)

	if err != nil {
		glog.Errorf("Error resumable uploading file filePath=%s err=%v", filePath, err)
		return 253, fmt.Errorf("error resumable uploading for assetId=%s taskId=%s: %w", uploadAsset.ID, uploadTask.ID, err)
	}

	err = vt.checkAssetProcessing(taskPollDuration, uploadAsset, uploadTask)

	if err != nil {
		glog.Errorf("Error processing asset assetId=%s taskId=%s", uploadAsset.ID, uploadTask.ID)
		return 254, err
	}

	glog.Info("Done VOD Test")

	return 0, nil
}

func (vt *vodTester) checkAssetProcessing(taskPollDuration time.Duration, uploadAsset api.Asset, uploadTask api.Task) error {
	startTime := time.Now()
	for {
		glog.Infof("Waiting %s assetId=%s, elapsed=%s", taskPollDuration, uploadAsset.ID, time.Since(startTime))
		time.Sleep(taskPollDuration)

		if err := vt.isCancelled(); err != nil {
			return err
		}

		asset, err := vt.lapi.GetAsset(uploadAsset.ID)
		if err != nil {
			glog.Errorf("Error retrieving asset id=%s err=%v", uploadAsset.ID, err)
			return fmt.Errorf("error retrieving asset id=%s: %w", uploadAsset.ID, err)
		}
		if asset.Status.Phase == "ready" {
			return nil
		}
		if asset.Status.Phase != "waiting" {
			glog.Errorf("Error task on asset assetId=%s, taskId=%s, status=%s, err=%v", asset.ID, uploadTask.ID, asset.Status.Phase, asset.Status.ErrorMessage)
			return fmt.Errorf("error task on asset assetId=%s, taskId=%s, status=%s, err=%v", asset.ID, uploadTask.ID, asset.Status.Phase, asset.Status.ErrorMessage)
		}
	}
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
