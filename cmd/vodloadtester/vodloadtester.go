package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang/glog"
	api "github.com/livepeer/go-api-client"
	"github.com/livepeer/go-api-client/logs"
	"github.com/livepeer/stream-tester/internal/utils"
	"github.com/peterbourgon/ff/v2"
)

type cliArguments struct {
	Verbosity    int
	Simultaneous uint
	Version      bool

	DirectUpload    bool
	ResumableUpload bool
	Import          bool

	TaskCheck         bool
	TaskCheckTimeout  uint
	TaskCheckPollTime uint
	APIServer         string
	APIToken          string
	Filename          string
	VideoAmount       uint
	OutputPath        string
	KeepAssets        bool

	TestDuration       time.Duration
	StartDelayDuration time.Duration
}

type vodLoadTester struct {
	ctx      context.Context
	cancel   context.CancelFunc
	lapi     *api.Client
	cliFlags cliArguments
}

type uploadTest struct {
	StartTime            time.Time `json:"startTime"`
	EndTime              time.Time `json:"endTime"`
	RunnerInfo           string    `json:"runnerInfo,omitempty"`
	Kind                 string    `json:"kind,omitempty"`
	AssetID              string    `json:"assetId,omitempty"`
	TaskID               string    `json:"taskId,omitempty"`
	RequestUploadSuccess uint      `json:"requestUploadSuccess,omitempty"`
	UploadSuccess        uint      `json:"uploadSuccess,omitempty"`
	ImportSuccess        uint      `json:"importSuccess,omitempty"`
	TaskCheckSuccess     uint      `json:"taskCheckSuccess,omitempty"`
	ErrorMessage         string    `json:"errorMessage,omitempty"`
	UrlSource            string    `json:"urlSource,omitempty"`
}

type jsonImport struct {
	Url string `json:"url"`
}

func main() {
	var cliFlags = &cliArguments{}

	flag.Set("logtostderr", "true")
	vFlag := flag.Lookup("v")

	fs := flag.NewFlagSet("vodloadtester", flag.ExitOnError)

	fs.IntVar(&cliFlags.Verbosity, "v", 5, "Log verbosity.  {4|5|6}")
	fs.BoolVar(&cliFlags.Version, "version", false, "Print out the version")

	// Credentials
	fs.StringVar(&cliFlags.APIToken, "api-token", "", "Token of the Livepeer API to be used")
	fs.StringVar(&cliFlags.APIServer, "api-server", "origin.livepeer.monster", "Server of the Livepeer API to be used")

	// Tests
	fs.BoolVar(&cliFlags.DirectUpload, "direct", false, "Launch direct upload test")
	fs.BoolVar(&cliFlags.ResumableUpload, "resumable", false, "Launch tus upload test")
	fs.BoolVar(&cliFlags.Import, "import", false, "Launch import from url test")

	// Input files and results
	fs.StringVar(&cliFlags.Filename, "file", "", "File to upload or url to import. Can be either a video or a .json array of objects with a url key")
	fs.StringVar(&cliFlags.OutputPath, "output-path", "/tmp/results.ndjson", "Path to output result .ndjson file")

	// Test parameters
	fs.UintVar(&cliFlags.VideoAmount, "video-amt", 1, "How many video to upload or import")
	fs.UintVar(&cliFlags.Simultaneous, "sim", 1, "Number of simulteneous videos to upload or import (batch size)")
	fs.DurationVar(&cliFlags.StartDelayDuration, "delay-between-groups", 10*time.Second, "Delay between starting group of uploads")

	// Task check parameters
	fs.BoolVar(&cliFlags.TaskCheck, "task-check", false, "Check task processing")
	fs.UintVar(&cliFlags.TaskCheckTimeout, "task-timeout", 500, "Task check timeout in seconds")
	fs.UintVar(&cliFlags.TaskCheckPollTime, "task-poll-time", 5, "Task check poll time in seconds")

	fs.BoolVar(&cliFlags.KeepAssets, "keep-assets", false, "Keep assets after test")

	_ = fs.String("config", "", "config file (optional)")

	ff.Parse(fs, os.Args[1:],
		ff.WithConfigFileFlag("config"),
		ff.WithConfigFileParser(ff.PlainParser),
		ff.WithEnvVarPrefix("VODLOADTESTER"),
	)
	flag.CommandLine.Parse(nil)
	vFlag.Value.Set(fmt.Sprintf("%d", cliFlags.Verbosity))

	hostName, _ := os.Hostname()
	runnerInfo := fmt.Sprintf("Hostname %s OS %s", hostName, runtime.GOOS)
	glog.V(logs.SHORT).Infof("Compiler version: %s %s\n", runtime.Compiler, runtime.Version())
	glog.V(logs.DEBUG).Infof(runnerInfo)

	if cliFlags.Version {
		return
	}

	if cliFlags.Filename == "" {
		glog.V(logs.SHORT).Infof("missing --file parameter")
		return
	}

	var err error
	var fileName string

	ctx, cancel := context.WithCancel(context.Background())

	if cliFlags.APIToken == "" {
		glog.V(logs.SHORT).Infof("No API token provided")
		return
	}

	apiToken := cliFlags.APIToken
	apiServer := cliFlags.APIServer

	lApiOpts := api.ClientOptions{
		Server:      apiServer,
		AccessToken: apiToken,
		Timeout:     240 * time.Second,
	}
	lapi, _ := api.NewAPIClientGeolocated(lApiOpts)
	vt := &vodLoadTester{
		lapi:     lapi,
		ctx:      ctx,
		cancel:   cancel,
		cliFlags: *cliFlags,
	}

	if cliFlags.Import {
		if cliFlags.DirectUpload || cliFlags.ResumableUpload {
			glog.V(logs.SHORT).Infof("Cannot use -import with either -direct or -resumable")
			return
		}
		fileName = cliFlags.Filename

		if strings.HasSuffix(fileName, ".json") {
			glog.V(logs.SHORT).Infof("Importing from json file %s. Ignoring any -video-amount parameter provided.", fileName)
			vt.importFromJSONTest(fileName, runnerInfo)
		} else {
			vt.importFromUrlTest(fileName, runnerInfo)
		}
		return
	}

	if cliFlags.DirectUpload || cliFlags.ResumableUpload {
		if fileName, err = utils.GetFile(cliFlags.Filename, strings.ReplaceAll(hostName, ".", "_")); err != nil {
			if err == utils.ErrNotFound {
				glog.V(logs.SHORT).Infof("file %s not found\n", cliFlags.Filename)
			} else {
				glog.V(logs.SHORT).Infof("error getting file %s: %v\n", cliFlags.Filename, err)
			}
		}

		if cliFlags.DirectUpload {
			glog.V(logs.DEBUG).Infof("Launching direct upload load test for %q", fileName)
			vt.directUploadLoadTest(fileName, runnerInfo)
		}
		if cliFlags.ResumableUpload {
			glog.V(logs.DEBUG).Infof("Launching resumable upload load test for %q", fileName)
			vt.resumableUploadLoadTest(fileName, runnerInfo)
		}
	}

}

func (vt *vodLoadTester) requestUploadUrls(assetName string) (*api.UploadUrls, error) {
	uploadUrls, err := vt.lapi.RequestUpload(assetName)
	if err != nil {
		return nil, err
	}
	return uploadUrls, nil
}

func (vt *vodLoadTester) directUploadLoadTest(fileName string, runnerInfo string) {
	wg := &sync.WaitGroup{}

	for i := 0; i < int(vt.cliFlags.VideoAmount); i += int(vt.cliFlags.Simultaneous) {
		for j := 0; j < int(vt.cliFlags.Simultaneous); j++ {
			glog.V(logs.SHORT).Infof("Uploading video %d/%d\n", i+j+1, vt.cliFlags.VideoAmount)
			wg.Add(1)
			go func() {
				vt.uploadFile(fileName, runnerInfo, wg, false)
			}()
		}
		time.Sleep(vt.cliFlags.StartDelayDuration)
	}

	wg.Wait()
}

func (vt *vodLoadTester) resumableUploadLoadTest(fileName, runnerInfo string) {
	wg := &sync.WaitGroup{}

	for i := 0; i < int(vt.cliFlags.VideoAmount); i += int(vt.cliFlags.Simultaneous) {
		for j := 0; j < int(vt.cliFlags.Simultaneous); j++ {
			glog.V(logs.SHORT).Infof("Uploading resumable video %d/%d\n", i+j+1, vt.cliFlags.VideoAmount)
			wg.Add(1)
			go func() {
				vt.uploadFile(fileName, runnerInfo, wg, true)
			}()
		}
		time.Sleep(vt.cliFlags.StartDelayDuration)
	}

	wg.Wait()

}

func (vt *vodLoadTester) uploadFile(fileName, runnerInfo string, wg *sync.WaitGroup, resumable bool) {

	uploadKind := "directUpload"

	if resumable {
		uploadKind = "resumableUpload"
	}

	uploadTest := uploadTest{
		StartTime:  time.Now(),
		RunnerInfo: runnerInfo,
		Kind:       uploadKind,
	}
	rndAssetName := fmt.Sprintf("load_test_%s_%s", uploadKind, randName())
	requestedUpload, err := vt.requestUploadUrls(rndAssetName)
	if !vt.cliFlags.KeepAssets {
		defer vt.lapi.DeleteAsset(requestedUpload.Asset.ID)
	}

	defer wg.Done()

	if err != nil {
		glog.V(logs.SHORT).Infof("Error requesting upload urls: %v", err)
		uploadTest.RequestUploadSuccess = 0
		uploadTest.ErrorMessage = err.Error()
		vt.writeResultNdjson(uploadTest)
		return
	} else {
		uploadTest.RequestUploadSuccess = 1
		uploadTest.AssetID = requestedUpload.Asset.ID
		uploadTest.TaskID = requestedUpload.Task.ID
	}

	uploadUrl := requestedUpload.Url

	if resumable {
		uploadUrl = requestedUpload.TusEndpoint
	}

	err = vt.doUpload(fileName, uploadUrl, resumable)

	if err != nil {
		glog.V(logs.SHORT).Infof("Error on %s: %v", uploadKind, err)
		uploadTest.UploadSuccess = 0
	} else {
		uploadTest.UploadSuccess = 1
		if vt.cliFlags.TaskCheck {
			err := vt.checkTaskProcessing(api.TaskOnlyId{ID: uploadTest.TaskID})
			if err != nil {
				uploadTest.TaskCheckSuccess = 0
				uploadTest.ErrorMessage = err.Error()
			} else {
				uploadTest.TaskCheckSuccess = 1
			}
		}

	}
	vt.writeResultNdjson(uploadTest)
}

func (vt *vodLoadTester) importFromJSONTest(jsonFile string, runnerInfo string) {
	data, err := ioutil.ReadFile(jsonFile)

	if err != nil {
		glog.V(logs.SHORT).Infof("Error reading json file: %v", err)
		return
	}

	var jsonData []jsonImport
	err = json.Unmarshal(data, &jsonData)

	if err != nil {
		glog.V(logs.SHORT).Infof("Error parsing json file: %v", err)
		return
	}

	wg := &sync.WaitGroup{}

	for i := 0; i < len(jsonData); i += int(vt.cliFlags.Simultaneous) {
		for j := 0; j < int(vt.cliFlags.Simultaneous); j++ {
			if i+j >= len(jsonData) {
				break
			}
			glog.V(logs.SHORT).Infof("Importing video from JSON, %d/%d\n", i+j+1, len(jsonData))
			wg.Add(1)
			index := i + j
			go func(i int) {
				vt.importFromUrl(jsonData[index].Url, runnerInfo, wg)
			}(i + j)
		}
		time.Sleep(vt.cliFlags.StartDelayDuration)
	}
	wg.Wait()
}

func (vt *vodLoadTester) importFromUrlTest(url string, runnerInfo string) {
	wg := &sync.WaitGroup{}

	for i := 0; i < int(vt.cliFlags.VideoAmount); i += int(vt.cliFlags.Simultaneous) {
		for j := 0; j < int(vt.cliFlags.Simultaneous); j++ {
			glog.V(logs.SHORT).Infof("Importing video %d/%d\n", i+j+1, vt.cliFlags.VideoAmount)
			wg.Add(1)
			go func() {
				vt.importFromUrl(url, runnerInfo, wg)
			}()
		}
		time.Sleep(vt.cliFlags.StartDelayDuration)
	}
	wg.Wait()
}

func (vt *vodLoadTester) importFromUrl(url string, runnerInfo string, wg *sync.WaitGroup) {
	uploadTest := uploadTest{
		StartTime:  time.Now(),
		RunnerInfo: runnerInfo,
		Kind:       "import",
		UrlSource:  url,
	}

	rndAssetName := fmt.Sprintf("load_test_import_%s", randName())
	glog.V(logs.DEBUG).Infof("Importing %s from %s", rndAssetName, url)
	asset, task, err := vt.lapi.ImportAsset(url, rndAssetName)
	if !vt.cliFlags.KeepAssets {
		defer vt.lapi.DeleteAsset(asset.ID)
	}

	defer wg.Done()

	if err != nil {
		glog.V(logs.SHORT).Infof("Error importing asset: %v", err)
		uploadTest.ImportSuccess = 0
		uploadTest.ErrorMessage = err.Error()
		vt.writeResultNdjson(uploadTest)
		return
	} else {
		uploadTest.ImportSuccess = 1
		uploadTest.AssetID = asset.ID
		uploadTest.TaskID = task.ID
	}

	if vt.cliFlags.TaskCheck {
		err := vt.checkTaskProcessing(api.TaskOnlyId{ID: uploadTest.TaskID})
		if err != nil {
			uploadTest.TaskCheckSuccess = 0
			uploadTest.ErrorMessage = err.Error()
		} else {
			uploadTest.TaskCheckSuccess = 1
		}
	}

	uploadTest.EndTime = time.Now()

	vt.writeResultNdjson(uploadTest)
}

func (vt *vodLoadTester) writeResultNdjson(uploadTest uploadTest) {
	uploadTest.EndTime = time.Now()
	jsonString, err := json.Marshal(uploadTest)
	if err != nil {
		glog.V(logs.SHORT).Infof("Error converting runTests to json: %v", err)
	}
	f, err := os.OpenFile(vt.cliFlags.OutputPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		glog.V(logs.SHORT).Infof("Error opening %s: %v", vt.cliFlags.OutputPath, err)
	}
	defer f.Close()
	if _, err := f.WriteString(string(jsonString) + "\n"); err != nil {
		glog.V(logs.SHORT).Infof("Error writing to %s: %v", vt.cliFlags.OutputPath, err)
	}
	glog.V(logs.SHORT).Infof("test results updated with new entry in %s", vt.cliFlags.OutputPath)
}

func (vt *vodLoadTester) doUpload(fileName string, uploadUrl string, resumable bool) error {

	file, err := os.Open(fileName)

	if err != nil {
		glog.V(logs.SHORT).Infof("error opening file %s: %v\n", fileName, err)
		return err
	}

	if resumable {
		err = vt.lapi.ResumableUpload(uploadUrl, file)
	} else {
		err = vt.lapi.UploadAsset(vt.ctx, uploadUrl, file)
	}

	if err != nil {
		glog.V(logs.SHORT).Infof("error uploading asset: %v\n", err)
		return err
	}

	return err
}

func (vt *vodLoadTester) checkTaskProcessing(processingTask api.TaskOnlyId) error {
	taskPollDuration := time.Duration(vt.cliFlags.TaskCheckPollTime) * time.Second
	startTime := time.Now()
	timeout := time.Duration(vt.cliFlags.TaskCheckTimeout) * time.Second
	for {

		time.Sleep(taskPollDuration)

		task, err := vt.lapi.GetTask(processingTask.ID)
		progress := ""
		if task.Status.Phase == "running" {
			percentage := task.Status.Progress * 100
			stringPercentage := strconv.FormatFloat(percentage, 'f', 2, 64)
			progress = fmt.Sprintf("progress=%s%%", stringPercentage)
		}

		glog.V(logs.DEBUG).Infof("Waiting %s for task id=%s to be processed, elapsed=%s %s", taskPollDuration, processingTask.ID, time.Since(startTime), progress)

		if err != nil {
			glog.Errorf("Error retrieving task id=%s err=%v", processingTask.ID, err)
			if strings.Contains(err.Error(), "connection reset by peer") || strings.Contains(err.Error(), "520") {
				// Retry
				glog.V(logs.SHORT).Infof("Livepeer Studio API unreachable, retrying getting task information.... ")
				continue
			}
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
			glog.Errorf("Internal timeout processing task, taskId=%s", task.ID)
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
