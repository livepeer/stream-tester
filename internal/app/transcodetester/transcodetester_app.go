package transcodetester

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/livepeer/stream-tester/internal/app/common"
	"math/rand"
	"net/http"
	"net/url"
	path2 "path"
	"strings"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/go-api-client"
	"golang.org/x/sync/errgroup"
)

type (
	ITranscodeTester interface {
		// Start test. Blocks until finished.
		Start(fileName, transcodeBucketUrl, transcodeW3sProof string, taskPollDuration time.Duration) error
		Cancel()
		Done() <-chan struct{}
	}

	transcodeTester struct {
		common.TesterApp
	}

	objectStore struct {
		accessKeyId     string
		secretAccessKey string
		endpoint        string
		bucket          string
	}
)

func NewTranscodeTester(gctx context.Context, opts common.TesterOptions) ITranscodeTester {
	ctx, cancel := context.WithCancel(gctx)
	vt := &transcodeTester{
		TesterApp: common.TesterApp{
			Lapi:       opts.API,
			Ctx:        ctx,
			CancelFunc: cancel,
		},
	}
	return vt
}

func (tt *transcodeTester) Start(fileName, transcodeBucketUrl, transcodeW3sProof string, taskPollDuration time.Duration) error {
	defer tt.Cancel()

	eg, egCtx := errgroup.WithContext(tt.Ctx)

	if transcodeBucketUrl != "" {
		eg.Go(func() error {
			if err := tt.transcodeS3FromUrlTester(fileName, transcodeBucketUrl, taskPollDuration); err != nil {
				glog.Errorf("Error in transcode S3 from url err=%v", err)
				return fmt.Errorf("error in transcode from url: %w", err)
			}
			return nil
		})

		eg.Go(func() error {
			if err := tt.transcodeS3FromPrivateBucketTester(fileName, transcodeBucketUrl, taskPollDuration); err != nil {
				glog.Errorf("Error in transcode S3 from private bucket err=%v", err)
				return fmt.Errorf("error in transcode from private bucket: %w", err)
			}
			return nil
		})
	}

	if transcodeW3sProof != "" {
		eg.Go(func() error {
			if err := tt.transcodeWeb3StorageTester(fileName, transcodeW3sProof, taskPollDuration); err != nil {
				glog.Errorf("Error in transcode web3.storage err=%v", err)
				return fmt.Errorf("error in transcode web3.storage: %w", err)
			}
			return nil
		})
	}

	go func() {
		<-egCtx.Done()
		tt.Cancel()
	}()
	if err := eg.Wait(); err != nil {
		return err
	}

	glog.Info("Done Transcode API Test")
	return nil
}

func (tt *transcodeTester) transcodeS3FromUrlTester(inUrl string, bucketUrl string, taskPollDuration time.Duration) error {
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
	return tt.checkTaskProcessingAndRenditionFiles(taskPollDuration, task, path, &os)
}

func (tt *transcodeTester) transcodeFromUrl(inUrl string, os objectStore, path string) (*api.Task, error) {
	return tt.Lapi.TranscodeFile(api.TranscodeFileReq{
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

func (tt *transcodeTester) transcodeS3FromPrivateBucketTester(inUrl string, bucketUrl string, taskPollDuration time.Duration) error {
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
		glog.Errorf("Error copying file into to input bucket=%s: err=%v", os.bucket, err)
		return fmt.Errorf("error copying file into input bucket=%s: %w", os.bucket, err)
	}

	task, err := tt.transcodeFromPrivateBucket(os, inPath, outPath)
	if err != nil {
		glog.Errorf("Error transcoding a file from private bucket=%s, path=%s: err=%v", os.bucket, inPath, err)
		return fmt.Errorf("error transcoding a file from private bucket=%s, path=%s: %w", os.bucket, inPath, err)
	}

	return tt.checkTaskProcessingAndRenditionFiles(taskPollDuration, task, outPath, &os)
}

func (tt *transcodeTester) transcodeFromPrivateBucket(os objectStore, inPath, outPath string) (*api.Task, error) {
	return tt.Lapi.TranscodeFile(api.TranscodeFileReq{
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

func (tt *transcodeTester) transcodeWeb3StorageTester(inUrl string, w3sProof string, taskPollDuration time.Duration) error {
	path := "/output"
	task, err := tt.transcodeWeb3StorageFromUrl(inUrl, w3sProof, path)
	if err != nil {
		glog.Errorf("Error transcoding a file to web3.storage from url=%s: err=%v", inUrl, err)
		return fmt.Errorf("error transcoding a file to web3.storage from url=%s: %w", inUrl, err)
	}
	return tt.checkTaskProcessingAndRenditionFiles(taskPollDuration, task, path, nil)
}

func (tt *transcodeTester) transcodeWeb3StorageFromUrl(inUrl, w3sProof, path string) (*api.Task, error) {
	return tt.Lapi.TranscodeFile(api.TranscodeFileReq{
		Input: api.TranscodeFileReqInput{
			Url: inUrl,
		},
		Storage: api.TranscodeFileReqStorage{
			Type: "web3.storage",
			Credentials: &api.TranscodeFileReqCredentials{
				Proof: w3sProof,
			},
		},
		Outputs: api.TranscodeFileReqOutputs{
			Hls: api.TranscodeFileReqOutputsHls{
				Path: path,
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

func (tt *transcodeTester) checkTaskProcessingAndRenditionFiles(taskPollDuration time.Duration, task *api.Task, path string, os *objectStore) error {
	rTask, err := tt.WaitTaskProcessing(taskPollDuration, *task)
	if err != nil {
		glog.Errorf("Error in transcoding task taskId=%s: err=%v", task.ID, err)
		return fmt.Errorf("error in transcoding task taskId=%s: %w", task.ID, err)
	}
	if err := tt.checkRenditionFiles(rTask, path, os); err != nil {
		glog.Errorf("Error in checking rendition segments in the bucket=%s, path=%s, err=%v", os.bucket, path, err)
		return fmt.Errorf("error in checking rendition segments in the bucket=%s, path=%s: %w", os.bucket, path, err)
	}
	return nil
}

func (tt *transcodeTester) checkRenditionFiles(task *api.Task, path string, os *objectStore) error {
	pathsToCheck := []string{path2.Join(path, "index.m3u8")}
	pathsToCheck = append(pathsToCheck, task.Output.TranscodeFile.Hls.Path)
	for _, m := range task.Output.TranscodeFile.Mp4 {
		pathsToCheck = append(pathsToCheck, m.Path)
	}

	if os != nil {
		// for Object Store systems check if files are accessible by the OS URL
		svc := s3.New(newAwsSession(*os))
		for _, p := range pathsToCheck {
			_, err := svc.GetObjectWithContext(tt.Ctx, &s3.GetObjectInput{
				Bucket: aws.String(os.bucket),
				Key:    aws.String(path2.Join(p)),
			})
			return err
		}
	}

	baseUrlStr := task.Output.TranscodeFile.BaseUrl
	if baseUrlStr != "" {
		// if baseUrl is returned, then a file should be accessible by this URL
		baseUrl, err := url.Parse(baseUrlStr)
		if err != nil {
			return err
		}
		if baseUrl.Scheme == "ipfs" {
			// for now, we only support IPFS via the web3.storage gateway
			w3sLinkBaseUrl, err := url.Parse(fmt.Sprintf("https://%s.ipfs.w3s.link/", baseUrl.Host))
			if err != nil {
				return err
			}
			for _, p := range pathsToCheck {
				u := w3sLinkBaseUrl.JoinPath(p)
				resp, err := http.Get(u.String())
				if err != nil {
					return err
				}
				if resp.StatusCode != http.StatusOK {
					return fmt.Errorf("cannot find rendition file: %s, HTTP status: %s", u, resp.Status)
				}
				resp.Body.Close()
			}
		}
	}

	return nil
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
