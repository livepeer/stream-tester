package vodtester

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/golang/glog"
	api "github.com/livepeer/go-api-client"
	"golang.org/x/sync/errgroup"
)

type (
	// IVodTester ...
	IVodTester interface {
		// Start start test. Blocks until finished.
		Start(fileName string, vodImportUrl string, taskPollDuration time.Duration) (int, error)
		Cancel()
		Done() <-chan struct{}
	}

	VodTesterOptions struct {
		API                      *api.Client
		CatalystPipelineStrategy string
	}

	vodTester struct {
		ctx                      context.Context
		cancel                   context.CancelFunc
		lapi                     *api.Client
		catalystPipelineStrategy string
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

func (vt *vodTester) Start(fileName string, vodImportUrl string, taskPollDuration time.Duration) (int, error) {
	defer vt.cancel()

	eg, egCtx := errgroup.WithContext(vt.ctx)

	eg.Go(func() error {

		hostName, _ := os.Hostname()
		assetName := fmt.Sprintf("vod_test_asset_%s_%s", hostName, time.Now().Format("2006-01-02T15:04:05Z07:00"))

		importAsset, err := vt.uploadViaUrlTester(vodImportUrl, taskPollDuration, assetName)

		if err != nil {
			glog.Errorf("Error importing asset from url=%s err=%v", vodImportUrl, err)
			return fmt.Errorf("error importing asset from url=%s: %w", vodImportUrl, err)
		}

		_, transcodeTask, err := vt.lapi.TranscodeAsset(importAsset.ID, assetName, api.StandardProfiles[0])

		if err != nil {
			glog.Errorf("Error transcoding asset assetId=%s err=%v", importAsset.ID, err)
			return fmt.Errorf("error transcoding asset assetId=%s: %w", importAsset.ID, err)
		}

		err = vt.checkTaskProcessing(taskPollDuration, *transcodeTask)

		if err != nil {
			glog.Errorf("Error in trasncoding task taskId=%s", transcodeTask.ID)
			return fmt.Errorf("error in transcoding task taskId=%s: %w", transcodeTask.ID, err)
		}

		exportTask, err := vt.lapi.ExportAsset(importAsset.ID)

		if err != nil {
			glog.Errorf("Error exporting asset assetId=%s err=%v", importAsset.ID, err)
			return fmt.Errorf("error exporting asset assetId=%s: %w", importAsset.ID, err)
		}

		err = vt.checkTaskProcessing(taskPollDuration, *exportTask)

		if err != nil {
			glog.Errorf("Error in export task taskId=%s", exportTask.ID)
			return fmt.Errorf("error in export task taskId=%s: %w", exportTask.ID, err)
		}

		return nil
	})

	eg.Go(func() error {
		err := vt.directUploadTester(fileName, taskPollDuration)

		if err != nil {
			glog.Errorf("Error in direct upload task err=%v", err)
			return fmt.Errorf("error in direct upload task: %w", err)
		}

		return nil
	})

	eg.Go(func() error {
		err := vt.resumableUploadTester(fileName, taskPollDuration)

		if err != nil {
			glog.Errorf("Error in resumable upload task err=%v", err)
			return fmt.Errorf("error in resumable upload task: %w", err)
		}

		return nil
	})
	go func() {
		<-egCtx.Done()
		vt.cancel()
	}()
	if err := eg.Wait(); err != nil {
		return 1, err
	}

	glog.Info("Done VOD Test")
	return 0, nil
}

func (vt *vodTester) uploadViaUrlTester(vodImportUrl string, taskPollDuration time.Duration, assetName string) (*api.Asset, error) {

	importAsset, importTask, err := vt.lapi.UploadViaURL(vodImportUrl, assetName, vt.catalystPipelineStrategy)
	if err != nil {
		glog.Errorf("Error importing asset err=%v", err)
		return nil, fmt.Errorf("error importing asset: %w", err)
	}
	glog.Infof("Importing asset taskId=%s outputAssetId=%s", importTask.ID, importAsset.ID)

	err = vt.checkTaskProcessing(taskPollDuration, *importTask)

	if err != nil {
		glog.Errorf("Error processing asset assetId=%s taskId=%s", importAsset.ID, importTask.ID)
	}
	return importAsset, err
}

func (vt *vodTester) directUploadTester(fileName string, taskPollDuration time.Duration) error {
	hostName, _ := os.Hostname()
	assetName := fmt.Sprintf("vod_test_upload_direct_%s_%s", hostName, time.Now().Format("2006-01-02T15:04:05Z07:00"))
	requestUpload, err := vt.lapi.RequestUpload(assetName, vt.catalystPipelineStrategy)

	if err != nil {
		glog.Errorf("Error requesting upload for assetName=%s err=%v", assetName, err)
		return fmt.Errorf("error requesting upload for assetName=%s: %w", assetName, err)
	}

	uploadEndpoint := requestUpload.Url
	uploadAsset := requestUpload.Asset
	uploadTask := api.Task{
		ID: requestUpload.Task.ID,
	}

	glog.Infof("Uploading to endpoint=%s", uploadEndpoint)

	file, err := os.Open(fileName)

	if err != nil {
		glog.Errorf("Error opening file=%s err=%v", fileName, err)
		return fmt.Errorf("error opening file=%s: %w", fileName, err)
	}

	err = vt.lapi.UploadAsset(vt.ctx, uploadEndpoint, file)
	if err != nil {
		glog.Errorf("Error uploading file filePath=%s err=%v", fileName, err)
		return fmt.Errorf("error uploading for assetId=%s taskId=%s: %w", uploadAsset.ID, uploadTask.ID, err)
	}

	err = vt.checkTaskProcessing(taskPollDuration, uploadTask)
	if err != nil {
		glog.Errorf("Error processing asset assetId=%s taskId=%s", uploadAsset.ID, uploadTask.ID)
	}
	return err
}

func (vt *vodTester) resumableUploadTester(fileName string, taskPollDuration time.Duration) error {

	hostName, _ := os.Hostname()
	assetName := fmt.Sprintf("vod_test_upload_resumable_%s_%s", hostName, time.Now().Format("2006-01-02T15:04:05Z07:00"))
	requestUpload, err := vt.lapi.RequestUpload(assetName, vt.catalystPipelineStrategy)

	if err != nil {
		glog.Errorf("Error requesting upload for assetName=%s err=%v", assetName, err)
		return fmt.Errorf("error requesting upload for assetName=%s: %w", assetName, err)
	}

	tusUploadEndpoint := patchURLHost(requestUpload.TusEndpoint, vt.lapi.GetServer())
	uploadAsset := requestUpload.Asset
	uploadTask := api.Task{
		ID: requestUpload.Task.ID,
	}

	file, err := os.Open(fileName)

	if err != nil {
		glog.Errorf("Error opening file=%s err=%v", fileName, err)
		return fmt.Errorf("error opening file=%s: %w", fileName, err)
	}

	err = vt.lapi.ResumableUpload(tusUploadEndpoint, file)

	if err != nil {
		glog.Errorf("Error resumable uploading file filePath=%s err=%v", fileName, err)
		return fmt.Errorf("error resumable uploading for assetId=%s taskId=%s: %w", uploadAsset.ID, uploadTask.ID, err)
	}

	err = vt.checkTaskProcessing(taskPollDuration, uploadTask)

	if err != nil {
		glog.Errorf("Error processing asset assetId=%s taskId=%s", uploadAsset.ID, uploadTask.ID)
	}

	return err
}

func (vt *vodTester) checkTaskProcessing(taskPollDuration time.Duration, processingTask api.Task) error {
	startTime := time.Now()
	for {
		glog.Infof("Waiting %s for task id=%s to be processed, elapsed=%s", taskPollDuration, processingTask.ID, time.Since(startTime))
		time.Sleep(taskPollDuration)

		if err := vt.isCancelled(); err != nil {
			return err
		}

		// we already sleep before the first check, so no need for strong consistency
		task, err := vt.lapi.GetTask(processingTask.ID, false)
		if err != nil {
			glog.Errorf("Error retrieving task id=%s err=%v", processingTask.ID, err)
			return fmt.Errorf("error retrieving task id=%s: %w", processingTask.ID, err)
		}
		if task.Status.Phase == "completed" {
			glog.Infof("Task success, taskId=%s", task.ID)
			return nil
		}
		if task.Status.Phase != "pending" && task.Status.Phase != "running" && task.Status.Phase != "waiting" {
			glog.Errorf("Error processing task, taskId=%s status=%s error=%v", task.ID, task.Status.Phase, task.Status.ErrorMessage)
			return fmt.Errorf("error processing task, taskId=%s status=%s error=%v", task.ID, task.Status.Phase, task.Status.ErrorMessage)
		}
	}
}

// Patches the target URL with the source URL host, only if the latter is not
// contained in the first. Used for doing resumable uploads to the same region
// under test.
func patchURLHost(target, src string) string {
	targetURL, err := url.Parse(target)
	if err != nil {
		return target
	}
	srcURL, err := url.Parse(src)
	if err != nil {
		return target
	}

	// Only patch the host if the target doesn't arleady contain the source host,
	// which would mean we are using a global endpoint for the API as well (e.g.
	// API server is livepeer.com and tus endpoint is origin.livepeer.com).
	if !strings.Contains(targetURL.Host, srcURL.Host) {
		targetURL.Scheme = srcURL.Scheme
		targetURL.Host = srcURL.Host
	}
	return targetURL.String()
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
