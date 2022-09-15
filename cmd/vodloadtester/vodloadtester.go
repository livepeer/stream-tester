package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/golang/glog"
	api "github.com/livepeer/go-api-client"
	"github.com/livepeer/stream-tester/internal/utils"
	"github.com/peterbourgon/ff/v2"
)

type cliArguments struct {
	Verbosity    int
	Simultaneous uint
	Version      bool
	TaskCheck    bool
	APIServer    string
	APIToken     string
	Filename     string
	VideoAmount  uint
	OutputPath   string

	TestDuration       time.Duration
	StartDelayDuration time.Duration
}

type vodLoadTester struct {
	ctx    context.Context
	cancel context.CancelFunc
	lapi   *api.Client
}

type uploadTest struct {
	StartTime            time.Time `json:"start_time"`
	EndTime              time.Time `json:"end_time"`
	RunnerInfo           string    `json:"runnerInfo,omitempty"`
	Kind                 string    `json:"kind,omitempty"`
	AssetID              string    `json:"assetId,omitempty"`
	TaskID               string    `json:"taskId,omitempty"`
	RequestUploadSuccess uint      `json:"requestUploadSuccess,omitempty"`
	UploadSuccess        uint      `json:"uploadSuccess,omitempty"`
	TaskCheckSuccess     uint      `json:"taskCheckSuccess,omitempty"`
	ErrorMessage         string    `json:"errorMessage,omitempty"`
}

func main() {
	var cliFlags = &cliArguments{}

	flag.Set("logtostderr", "true")
	vFlag := flag.Lookup("v")

	fs := flag.NewFlagSet("loadtester", flag.ExitOnError)

	fs.IntVar(&cliFlags.Verbosity, "v", 3, "Log verbosity.  {4|5|6}")
	fs.BoolVar(&cliFlags.Version, "version", false, "Print out the version")
	fs.BoolVar(&cliFlags.TaskCheck, "task-check", false, "Check task processing")

	fs.UintVar(&cliFlags.VideoAmount, "video-amt", 1, "How many video to upload")
	fs.DurationVar(&cliFlags.StartDelayDuration, "delay-between-groups", 10*time.Second, "Delay between starting group of uploads")

	fs.UintVar(&cliFlags.Simultaneous, "sim", 1, "Number of simulteneous videos to upload")
	fs.StringVar(&cliFlags.Filename, "file", "bbb_sunflower_1080p_30fps_normal_2min.mp4", "File to upload")
	fs.StringVar(&cliFlags.APIToken, "api-token", "", "Token of the Livepeer API to be used")
	fs.StringVar(&cliFlags.APIServer, "api-server", "origin.livepeer.com", "Server of the Livepeer API to be used")
	fs.StringVar(&cliFlags.OutputPath, "output-path", "/tmp/results.ndjson", "Path to output result .ndjson file")

	_ = fs.String("config", "", "config file (optional)")

	ff.Parse(fs, os.Args[1:],
		ff.WithConfigFileFlag("config"),
		ff.WithConfigFileParser(ff.PlainParser),
		ff.WithEnvVarPrefix("VODLOADTESTER"),
	)
	flag.CommandLine.Parse(nil)
	vFlag.Value.Set(fmt.Sprintf("%d", cliFlags.Verbosity))

	hostName, _ := os.Hostname()
	runnerInfo := fmt.Sprintf("Hostname %s OS %s IPs %v\n", hostName, runtime.GOOS, utils.GetIPs())
	fmt.Printf("Compiler version: %s %s\n", runtime.Compiler, runtime.Version())
	fmt.Print(runnerInfo)

	if cliFlags.Version {
		return
	}

	if cliFlags.Filename == "" {
		glog.Fatal("missing --file parameter")
	}
	var err error
	var fileName string

	if fileName, err = utils.GetFile(cliFlags.Filename, strings.ReplaceAll(hostName, ".", "_")); err != nil {
		if err == utils.ErrNotFound {
			glog.Fatalf("file %s not found\n", cliFlags.Filename)
		} else {
			glog.Fatalf("error getting file %s: %v\n", cliFlags.Filename, err)
		}
	}
	glog.Infof("uploading video file %q", fileName)

	ctx, cancel := context.WithCancel(context.Background())

	if cliFlags.APIToken == "" {
		glog.Fatalf("No API token provided")
	}

	apiToken := cliFlags.APIToken
	apiServer := cliFlags.APIServer
	outputNdjson := cliFlags.OutputPath

	lApiOpts := api.ClientOptions{
		Server:      apiServer,
		AccessToken: apiToken,
		Timeout:     240 * time.Second,
	}
	lapi, _ := api.NewAPIClientGeolocated(lApiOpts)
	vt := &vodLoadTester{
		lapi:   lapi,
		ctx:    ctx,
		cancel: cancel,
	}

	wg := &sync.WaitGroup{}

	for i := 0; i < int(cliFlags.VideoAmount); i += int(cliFlags.Simultaneous) {
		for j := 0; j < int(cliFlags.Simultaneous); j++ {
			fmt.Printf("Uploading video %d/%d\n", i+j+1, cliFlags.VideoAmount)
			wg.Add(1)
			go func() {
				uploadTest := uploadTest{
					StartTime:  time.Now(),
					RunnerInfo: runnerInfo,
					Kind:       "directUpload",
				}
				assetId, taskId, err := vt.uploadAsset(fileName)
				if err != nil {
					glog.Errorf("Error uploading asset: %v", err)
					if assetId == "" {
						uploadTest.RequestUploadSuccess = 0
						uploadTest.ErrorMessage = err.Error()
					} else {
						uploadTest.AssetID = assetId
						uploadTest.RequestUploadSuccess = 1
						uploadTest.UploadSuccess = 0
					}
				} else {
					uploadTest.AssetID = assetId
					uploadTest.TaskID = taskId
					uploadTest.RequestUploadSuccess = 1
					uploadTest.UploadSuccess = 1
					if cliFlags.TaskCheck {
						err := vt.checkTaskProcessing(5*time.Second, api.TaskOnlyId{ID: uploadTest.TaskID})
						if err != nil {
							uploadTest.TaskCheckSuccess = 0
							uploadTest.ErrorMessage = err.Error()
						} else {
							uploadTest.TaskCheckSuccess = 1
						}
					}

					uploadTest.EndTime = time.Now()
					jsonString, err := json.Marshal(uploadTest)
					if err != nil {
						glog.Errorf("Error converting runTests to json: %v", err)
					}
					f, err := os.OpenFile(outputNdjson, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
					if err != nil {
						glog.Errorf("Error opening %s: %v", outputNdjson, err)
					}
					defer f.Close()
					if _, err := f.WriteString(string(jsonString) + "\n"); err != nil {
						glog.Errorf("Error writing to %s: %v", outputNdjson, err)
					}

				}
				wg.Done()
			}()
		}
		time.Sleep(cliFlags.StartDelayDuration)
	}

	wg.Wait()

}

func (vt *vodLoadTester) uploadAsset(fileName string) (string, string, error) {
	rndAssetName := fmt.Sprintf("load_test_%s", randName())

	res, err := vt.lapi.RequestUpload(rndAssetName)
	if err != nil {
		fmt.Printf("error requesting upload: %v\n", err)
		return "", "", err
	}
	uploadUrl := res.Url
	assetId := res.Asset.ID
	taskId := res.Task.ID

	file, err := os.Open(fileName)

	if err != nil {
		fmt.Printf("error opening file %s: %v\n", fileName, err)
		return assetId, taskId, err
	}

	err = vt.lapi.UploadAsset(vt.ctx, uploadUrl, file)
	if err != nil {
		fmt.Printf("error uploading asset: %v\n", err)
		return assetId, taskId, err
	}

	fmt.Printf("Video uploaded for asset id %s\n", assetId)

	return assetId, taskId, err
}

func (vt *vodLoadTester) checkTaskProcessing(taskPollDuration time.Duration, processingTask api.TaskOnlyId) error {
	startTime := time.Now()
	timeout := 3 * time.Minute
	for {
		glog.Infof("Waiting %s for task id=%s to be processed, elapsed=%s", taskPollDuration, processingTask.ID, time.Since(startTime))
		time.Sleep(taskPollDuration)

		task, err := vt.lapi.GetTask(processingTask.ID)
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

		if time.Since(startTime) > timeout {
			glog.Errorf("Timeout processing task, taskId=%s", task.ID)
			return fmt.Errorf("timeout processing task, taskId=%s", task.ID)
		}
	}
}

func randName() string {
	x := make([]byte, 10)
	for i := 0; i < len(x); i++ {
		x[i] = byte(rand.Uint32())
	}
	return fmt.Sprintf("%x", x)
}
