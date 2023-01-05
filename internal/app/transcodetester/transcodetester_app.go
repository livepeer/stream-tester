package transcodetester

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"math/rand"
	"net/http"
	"net/url"
	path2 "path"
	"strings"
	"time"

	"github.com/golang/glog"
	api "github.com/livepeer/go-api-client"
	"golang.org/x/sync/errgroup"
)

type (
	ITranscodeTester interface {
		// Start test. Blocks until finished.
		Start(fileName string, transcodeBucketUrl string, taskPollDuration time.Duration) (int, error)
		Cancel()
		Done() <-chan struct{}
	}

	TranscodeTesterOptions struct {
		API                      *api.Client
		CatalystPipelineStrategy string
	}

	transcodeTester struct {
		ctx                      context.Context
		cancel                   context.CancelFunc
		lapi                     *api.Client
		catalystPipelineStrategy string
	}

	objectStore struct {
		accessKeyId     string
		secretAccessKey string
		endpoint        string
		bucket          string
	}
)

func NewTranscodeTester(gctx context.Context, opts TranscodeTesterOptions) ITranscodeTester {
	ctx, cancel := context.WithCancel(gctx)
	vt := &transcodeTester{
		lapi:   opts.API,
		ctx:    ctx,
		cancel: cancel,
	}
	return vt
}

func (tt *transcodeTester) Start(fileName string, transcodeBucketUrl string, taskPollDuration time.Duration) (int, error) {
	defer tt.cancel()

	eg, egCtx := errgroup.WithContext(tt.ctx)

	eg.Go(func() error {
		if err := tt.transcodeFromUrlTester(fileName, transcodeBucketUrl, taskPollDuration); err != nil {
			glog.Errorf("Error in transcode from url err=%v", err)
			return fmt.Errorf("error in transcode from url: %w", err)
		}
		return nil
	})

	eg.Go(func() error {
		if err := tt.transcodeFromPrivateBucketTester(fileName, transcodeBucketUrl, taskPollDuration); err != nil {
			glog.Errorf("Error in transcode from private bucket err=%v", err)
			return fmt.Errorf("error in transcode from private bucket: %w", err)
		}
		return nil
	})

	go func() {
		<-egCtx.Done()
		tt.cancel()
	}()
	if err := eg.Wait(); err != nil {
		return 1, err
	}

	glog.Info("Done Transcode API Test")
	return 0, nil
}

func (tt *transcodeTester) transcodeFromUrlTester(inUrl string, bucketUrl string, taskPollDuration time.Duration) error {
	os, err := parseObjectStore(bucketUrl)
	if err != nil {
		glog.Errorf("Error parsing bucket url=%s: err=%v", bucketUrl, err)
		return fmt.Errorf("error parsing bucket url=%s: %w", bucketUrl, err)
	}
	path := path2.Join("/output", randomPath())

	task, err := tt.transcodeFromUrl(inUrl, os, path)
	if err != nil {
		glog.Errorf("Error transcoding a file from url=%s: err=%v", inUrl, err)
		return fmt.Errorf("error transcoding a file from url=%s: %w", inUrl, err)
	}
	return tt.checkTaskProcessingAndRenditionFiles(taskPollDuration, *task, os, path)
}

func (tt *transcodeTester) transcodeFromUrl(inUrl string, os objectStore, path string) (*api.Task, error) {
	return tt.lapi.TranscodeFile(api.TranscodeFileReq{
		Input: api.TranscodeFileReqInput{
			Url: inUrl,
		},
		Storage: api.TranscodeFileReqStorage{
			Type:     "s3",
			Endpoint: os.endpoint,
			Credentials: &api.TranscodeFileReqCredentials{
				AccessKeyId:     os.accessKeyId,
				SecretAccessKey: os.secretAccessKey,
			},
			Bucket: os.bucket,
		},
		Outputs: api.TranscodeFileReqOutputs{
			Hls: api.TranscodeFileReqOutputsHls{
				Path: path,
			},
		},
	})
}

func (tt *transcodeTester) transcodeFromPrivateBucketTester(inUrl string, bucketUrl string, taskPollDuration time.Duration) error {
	url, err := url.Parse(inUrl)
	if err != nil {
		glog.Errorf("Error parsing input file url=%s: err=%v", inUrl, err)
		return fmt.Errorf("error parsing input url=%s: %w", inUrl, err)
	}
	os, err := parseObjectStore(bucketUrl)
	if err != nil {
		glog.Errorf("Error parsing bucket url=%s: err=%v", bucketUrl, err)
		return fmt.Errorf("error parsing bucket url=%s: %w", bucketUrl, err)
	}

	randPath := randomPath()
	inPath := path2.Join("/input", randPath, "source"+path2.Ext(url.Path))
	outPath := path2.Join("/output", randPath)

	if err := tt.copyFileIntoInputBucket(inUrl, os, inPath); err != nil {
		glog.Errorf("Error copying file into input bucket=%s: err=%v", os.bucket, err)
		return fmt.Errorf("error copying file into input bucket=%s: %w", os.bucket, err)
	}

	task, err := tt.transcodeFromPrivateBucket(os, inPath, outPath)
	if err != nil {
		glog.Errorf("Error transcoding a file from private bucket=%s, path=%s: err=%v", os.bucket, inPath, err)
		return fmt.Errorf("error transcoding a file from private bucket=%s, path=%s: %w", os.bucket, inPath, err)
	}

	return tt.checkTaskProcessingAndRenditionFiles(taskPollDuration, *task, os, outPath)
}

func (tt *transcodeTester) transcodeFromPrivateBucket(os objectStore, inPath, outPath string) (*api.Task, error) {
	return tt.lapi.TranscodeFile(api.TranscodeFileReq{
		Input: api.TranscodeFileReqInput{
			Type:     "s3",
			Endpoint: os.endpoint,
			Credentials: &api.TranscodeFileReqCredentials{
				AccessKeyId:     os.accessKeyId,
				SecretAccessKey: os.secretAccessKey,
			},
			Bucket: os.bucket,
			Path:   inPath,
		},
		Storage: api.TranscodeFileReqStorage{
			Type:     "s3",
			Endpoint: os.endpoint,
			Credentials: &api.TranscodeFileReqCredentials{
				AccessKeyId:     os.accessKeyId,
				SecretAccessKey: os.secretAccessKey,
			},
			Bucket: os.bucket,
		},
		Outputs: api.TranscodeFileReqOutputs{
			Hls: api.TranscodeFileReqOutputsHls{
				Path: outPath,
			},
		},
	})
}

func (tt *transcodeTester) copyFileIntoInputBucket(inUrl string, os objectStore, path string) error {
	uploader := s3manager.NewUploader(newAwsSession(os))
	resp, err := http.Get(inUrl)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(os.bucket),
		Key:    aws.String(path),
		Body:   resp.Body,
	})
	return err
}

func (tt *transcodeTester) checkTaskProcessingAndRenditionFiles(taskPollDuration time.Duration, task api.Task, os objectStore, path string) error {
	if err := tt.checkTaskProcessing(taskPollDuration, task); err != nil {
		glog.Errorf("Error in transcoding task taskId=%s: err=%v", task.ID, err)
		return fmt.Errorf("error in transcoding task taskId=%s: %w", task.ID, err)
	}
	if err := tt.checkRenditionFiles(os, path); err != nil {
		glog.Errorf("Error in checking rendition segments in the bucket=%s, path=%s, err=%v", os.bucket, path, err)
		return fmt.Errorf("error in checking rendition segments in the bucket=%s, path=%s: %w", os.bucket, path, err)
	}
	return nil
}

func (tt *transcodeTester) checkTaskProcessing(taskPollDuration time.Duration, processingTask api.Task) error {
	startTime := time.Now()
	for {
		time.Sleep(taskPollDuration)

		if err := tt.isCancelled(); err != nil {
			return err
		}

		// we already sleep before the first check, so no need for strong consistency
		task, err := tt.lapi.GetTask(processingTask.ID, false)
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

		glog.Infof("Waiting for task to be processed id=%s pollWait=%s elapsed=%s progressPct=%.1f%%", task.ID, taskPollDuration, time.Since(startTime), 100*task.Status.Progress)
	}
}

func (tt *transcodeTester) checkRenditionFiles(os objectStore, path string) error {
	svc := s3.New(newAwsSession(os))
	_, err := svc.GetObjectWithContext(tt.ctx, &s3.GetObjectInput{
		Bucket: aws.String(os.bucket),
		Key:    aws.String(path2.Join(path, "index.m3u8")),
	})
	return err
}

func (tt *transcodeTester) isCancelled() error {
	select {
	case <-tt.ctx.Done():
		return context.Canceled
	default:
	}
	return nil
}

func (tt *transcodeTester) Cancel() {
	tt.cancel()
}

func (tt *transcodeTester) Done() <-chan struct{} {
	return tt.ctx.Done()
}

func parseObjectStore(bucketUrl string) (objectStore, error) {
	url, err := url.Parse(bucketUrl)
	if err != nil {
		return objectStore{}, err
	}

	os := objectStore{}
	os.accessKeyId = url.User.Username()
	os.secretAccessKey, _ = url.User.Password()
	os.bucket = strings.TrimPrefix(url.Path, "/")
	os.endpoint = fmt.Sprintf("%s://%s", strings.TrimPrefix(url.Scheme, "s3+"), url.Host)
	return os, nil
}

func newAwsSession(os objectStore) *session.Session {
	region := "unused"
	return session.Must(session.NewSession(&aws.Config{
		Endpoint:    &os.endpoint,
		Credentials: credentials.NewStaticCredentials(os.accessKeyId, os.secretAccessKey, ""),
		Region:      &region,
	}))
}

func randomPath() string {
	const length = 10
	const charset = "abcdefghijklmnopqrstuvwxyz0123456789"
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	res := make([]byte, length)
	for i := 0; i < length; i++ {
		res[i] = charset[r.Intn(length)]
	}
	return "/" + string(res)
}
