package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
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
	Clip            bool

	TaskCheck         bool
	TaskCheckTimeout  uint
	TaskCheckPollTime uint
	APIServer         string
	APIToken          string
	Filename          string
	Folder            string
	VideoAmount       uint
	OutputPath        string
	KeepAssets        bool
	ProbeData         bool

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
	ApiServer            string      `json:"apiServer"`
	StartTime            string      `json:"startTime"`
	EndTime              string      `json:"endTime"`
	RunnerInfo           string      `json:"runnerInfo,omitempty"`
	Kind                 string      `json:"kind,omitempty"`
	AssetID              string      `json:"assetId,omitempty"`
	TaskID               string      `json:"taskId,omitempty"`
	RequestUploadSuccess uint        `json:"requestUploadSuccess,omitempty"`
	UploadSuccess        uint        `json:"uploadSuccess,omitempty"`
	ImportSuccess        uint        `json:"importSuccess,omitempty"`
	TaskCheckSuccess     uint        `json:"taskCheckSuccess,omitempty"`
	ErrorMessage         string      `json:"errorMessage,omitempty"`
	Source               string      `json:"source,omitempty"`
	Elapsed              uint        `json:"elapsed,omitempty"`
	TimeToSourceReady    float64     `json:"timeToSourceReady,omitempty"`
	ProbeData            []ProbeData `json:"data,omitempty"`
}

// This is specifically for JSON imported files with these fields into the json objects
// Probably temporary for the new pipeline VOD test, may be removed later or may be done in a different way (like ffprobe standard output)
type ProbeData struct {
	AssetName       string `json:"asset_name,omitempty"`
	Size            string `json:"size,omitempty"`
	ContainerFormat string `json:"container_format,omitempty"`
	VideoTracks     string `json:"video_tracks,omitempty"`
	AudioTracks     string `json:"audio_tracks,omitempty"`
	SubtitleTracks  string `json:"subtitle_tracks,omitempty"`
	OtherTracks     string `json:"other_tracks,omitempty"`
	VideoCodec      string `json:"video_codec,omitempty"`
	AudioCodec      string `json:"audio_codec,omitempty"`
	PixelFormat     string `json:"pixel_format,omitempty"`
	Duration        string `json:"duration,omitempty"`
	Width           string `json:"width,omitempty"`
	Height          string `json:"height,omitempty"`
	Fps             string `json:"fps,omitempty"`
	URL             string `json:"url,omitempty"`
}

type jsonImport struct {
	Url string `json:"url"`
}

var videoFormats = []string{
	"mp4",
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
	fs.BoolVar(&cliFlags.Clip, "clip", false, "Launch clipping load test")

	// Input files and results
	fs.StringVar(&cliFlags.Filename, "file", "", "File to upload or url to import. Can be either a video or a .json array of objects with a url key")
	fs.StringVar(&cliFlags.Folder, "folder", "", "Folder with files to upload. The tester will search in the folder for files with the following extensions: "+strings.Join(videoFormats, ", "))
	fs.StringVar(&cliFlags.OutputPath, "output-path", "/tmp/results.ndjson", "Path to output result .ndjson file")
	fs.BoolVar(&cliFlags.ProbeData, "probe-data", false, "Write object data in results when importing a JSON file")

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

	if cliFlags.Filename == "" && cliFlags.Folder == "" && !cliFlags.Clip {
		glog.V(logs.SHORT).Infof("missing --file or --folder parameter")
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

	// Import from URL or JSON load test
	if cliFlags.Import {
		if cliFlags.DirectUpload || cliFlags.ResumableUpload {
			glog.V(logs.SHORT).Infof("Cannot use -import with either -direct or -resumable")
			return
		}

		if cliFlags.Filename == "" {
			glog.V(logs.SHORT).Infof("Cannot use -import without specifying a -file $URL or -file $JSON_FILE")
			return
		}

		fileName = cliFlags.Filename

		if strings.HasSuffix(fileName, ".json") {
			glog.V(logs.SHORT).Infof("Importing from json file %s. Ignoring any -video-amt parameter provided.", fileName)
			vt.importFromJSONTest(fileName, runnerInfo)
		} else {
			vt.importFromUrlTest(fileName, runnerInfo)
		}
		return
	}

	// Upload tests
	if cliFlags.DirectUpload || cliFlags.ResumableUpload {

		// From folder
		if cliFlags.Folder != "" {
			glog.V(logs.SHORT).Infof("Uploading from folder %s. Ignoring any -video-amt and -file parameter provided.", cliFlags.Folder)
			if cliFlags.DirectUpload {
				glog.V(logs.DEBUG).Infof("Launching direct upload test for folder %s", cliFlags.Folder)
				vt.directUploadFromFolderLoadTest(cliFlags.Folder, runnerInfo)
			}
			if cliFlags.ResumableUpload {
				glog.V(logs.DEBUG).Infof("Launching resumable upload test for folder %s", cliFlags.Folder)
				vt.resumableUploadFromFolderLoadTest(cliFlags.Folder, runnerInfo)
			}
		} else {
			// From file
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

	if cliFlags.Clip {
		playbackId := "afb20wapuh9cubwx"
		vt.clipTest(playbackId, runnerInfo)
	}

}

func (vt *vodLoadTester) requestUploadUrls(assetName string) (*api.UploadUrls, error) {
	uploadUrls, err := vt.lapi.RequestUpload(assetName, "")
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

func (vt *vodLoadTester) directUploadFromFolderLoadTest(folderName string, runnerInfo string) {
	wg := &sync.WaitGroup{}

	filePaths := vt.getVideoFilesFromFolder(folderName)

	if len(filePaths) == 0 {
		glog.V(logs.SHORT).Infof("No files with extension %v found in folder %s\n", videoFormats, folderName)
		return
	}

	for i := 0; i < len(filePaths); i += int(vt.cliFlags.Simultaneous) {
		for j := 0; j < int(vt.cliFlags.Simultaneous); j++ {
			if i+j >= len(filePaths) {
				break
			}
			glog.V(logs.DEBUG).Infof("Uploading video from folder, %d/%d\n", i+j+1, len(filePaths))
			wg.Add(1)
			index := i + j
			go func() {
				vt.uploadFile(filePaths[index], runnerInfo, wg, false)
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

func (vt *vodLoadTester) clipTest(playbackId string, runnerInfo string) {

	wg := &sync.WaitGroup{}

	for i := 0; i < int(vt.cliFlags.VideoAmount); i += int(vt.cliFlags.Simultaneous) {
		for j := 0; j < int(vt.cliFlags.Simultaneous); j++ {
			glog.V(logs.SHORT).Infof("Clipping video %d/%d\n", i+j+1, vt.cliFlags.VideoAmount)
			wg.Add(1)
			go func() {
				vt.Clip(playbackId, runnerInfo, wg)
			}()
		}
		time.Sleep(vt.cliFlags.StartDelayDuration)
	}

	wg.Wait()
}

func (vt *vodLoadTester) resumableUploadFromFolderLoadTest(folderName string, runnerInfo string) {
	wg := &sync.WaitGroup{}

	filePaths := vt.getVideoFilesFromFolder(folderName)

	if len(filePaths) == 0 {
		glog.V(logs.SHORT).Infof("No files with extension %v found in folder %s\n", videoFormats, folderName)
		return
	}

	for i := 0; i < len(filePaths); i += int(vt.cliFlags.Simultaneous) {
		for j := 0; j < int(vt.cliFlags.Simultaneous); j++ {
			if i+j >= len(filePaths) {
				break
			}
			glog.V(logs.DEBUG).Infof("Uploading video from folder, %d/%d\n", i+j+1, len(filePaths))
			wg.Add(1)
			index := i + j
			go func() {
				vt.uploadFile(filePaths[index], runnerInfo, wg, true)
			}()
		}
		time.Sleep(vt.cliFlags.StartDelayDuration)
	}
	wg.Wait()
}

func (vt *vodLoadTester) getVideoFilesFromFolder(folderName string) []string {
	files, err := ioutil.ReadDir(folderName)
	var filePaths []string

	if err != nil {
		glog.V(logs.SHORT).Infof("Error reading folder %s: %v\n", folderName, err)
		return filePaths
	}

	glog.V(logs.SHORT).Infof("Uploading %d videos from folder %s\n", len(files), folderName)

	for _, file := range files {
		if file.IsDir() {
			continue
		}
		for _, format := range videoFormats {
			if strings.HasSuffix(file.Name(), format) {
				glog.V(logs.DEBUG).Infof("Uploading video %s\n", file.Name())
				filePaths = append(filePaths, filepath.Join(folderName, file.Name()))
			}
		}
	}

	return filePaths
}

func (vt *vodLoadTester) uploadFile(fileName, runnerInfo string, wg *sync.WaitGroup, resumable bool) {

	uploadKind := "directUpload"

	if resumable {
		uploadKind = "resumableUpload"
	}
	startTime := time.Now()
	uploadTest := uploadTest{
		ApiServer:  vt.cliFlags.APIServer,
		StartTime:  startTime.Format("2006-01-02 15:04:05"),
		RunnerInfo: runnerInfo,
		Kind:       uploadKind,
		Source:     fileName,
	}
	rndAssetName := fmt.Sprintf("load_test_%s_%s", uploadKind, randName())
	requestedUpload, err := vt.requestUploadUrls(rndAssetName)

	defer wg.Done()

	if err != nil {
		glog.V(logs.SHORT).Infof("Error requesting upload urls: %v", err)
		uploadTest.RequestUploadSuccess = 0
		uploadTest.ErrorMessage = err.Error()
		vt.writeResultNdjson(uploadTest, startTime)
		return
	} else {
		uploadTest.RequestUploadSuccess = 1
		uploadTest.AssetID = requestedUpload.Asset.ID
		uploadTest.TaskID = requestedUpload.Task.ID
	}

	if !vt.cliFlags.KeepAssets {
		defer vt.lapi.DeleteAsset(requestedUpload.Asset.ID)
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
			err := vt.checkTaskProcessing(api.TaskOnlyId{ID: uploadTest.TaskID}, uploadTest.AssetID)
			if err != nil {
				uploadTest.TaskCheckSuccess = 0
				uploadTest.ErrorMessage = err.Error()
			} else {
				uploadTest.TaskCheckSuccess = 1
			}
		}

	}
	vt.writeResultNdjson(uploadTest, startTime)
}

func (vt *vodLoadTester) Clip(playbackId string, runnerInfo string, wg *sync.WaitGroup) {

	rndAssetName := fmt.Sprintf("clip_test_%s", randName())
	startTime := time.Now()
	uploadTest := uploadTest{
		ApiServer:  vt.cliFlags.APIServer,
		StartTime:  startTime.Format("2006-01-02 15:04:05"),
		RunnerInfo: runnerInfo,
		Kind:       "clip",
		Source:     "",
	}

	asset, task, err := vt.lapi.Clip(api.Clip{
		PlaybackID: playbackId,
		StartTime:  1697641672000,
		EndTime:    1697641852000,
		Name:       rndAssetName,
	})

	uploadTest.TaskID = task.ID
	uploadTest.AssetID = asset.ID

	if err != nil {
		glog.V(logs.SHORT).Infof("Error clipping asset: %v", err)
		uploadTest.RequestUploadSuccess = 0
		uploadTest.ErrorMessage = err.Error()
		vt.writeResultNdjson(uploadTest, startTime)
		return
	}

	uploadTest.RequestUploadSuccess = 1

	// Create a new WaitGroup to wait for parallel operations
	var innerWg sync.WaitGroup

	// Channel to communicate errors between goroutines
	errCh := make(chan error, 2)

	// Check asset source in a goroutine
	innerWg.Add(1)
	go func() {
		defer innerWg.Done()

		err := vt.checkAssetSource(asset.ID)
		if err != nil {
			uploadTest.TaskCheckSuccess = 0
			uploadTest.ErrorMessage = err.Error()
			errCh <- err
		} else {
			uploadTest.TimeToSourceReady = time.Since(startTime).Seconds()
		}
	}()

	// Check task processing in another goroutine
	innerWg.Add(1)
	go func() {
		defer innerWg.Done()

		err := vt.checkTaskProcessing(api.TaskOnlyId{ID: task.ID}, uploadTest.AssetID)
		if err != nil {
			uploadTest.TaskCheckSuccess = 0
			uploadTest.ErrorMessage = err.Error()
			errCh <- err
		}
	}()

	// Wait for the goroutines to finish
	innerWg.Wait()

	// Check for errors from goroutines and write result
	if len(errCh) > 0 {
		// Here, you can handle the errors from the goroutines if you wish.
		// For now, we're simply logging the first error.
		glog.V(logs.SHORT).Infof("Error from goroutines: %v", <-errCh)
	}

	vt.writeResultNdjson(uploadTest, startTime)

	defer wg.Done()
}

func (vt *vodLoadTester) importFromJSONTest(jsonFile string, runnerInfo string) {
	data, err := ioutil.ReadFile(jsonFile)

	if err != nil {
		glog.V(logs.SHORT).Infof("Error reading json file: %v", err)
		return
	}

	var jsonData []jsonImport
	var probeData []ProbeData
	err = json.Unmarshal(data, &jsonData)

	if err != nil {
		glog.V(logs.SHORT).Infof("Error parsing json file: %v", err)
		return
	}

	err = json.Unmarshal(data, &probeData)

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
				var probe *ProbeData

				if index < len(probeData) {
					probe = &probeData[index]
				}

				vt.importFromUrl(jsonData[index].Url, runnerInfo, wg, *probe)
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

func (vt *vodLoadTester) importFromUrl(url string, runnerInfo string, wg *sync.WaitGroup, probeData ...ProbeData) {
	startTime := time.Now()
	uploadTest := uploadTest{
		ApiServer:  vt.cliFlags.APIServer,
		StartTime:  startTime.Format("2006-01-02 15:04:05"),
		RunnerInfo: runnerInfo,
		Kind:       "import",
		Source:     url,
	}

	if probeData != nil && vt.cliFlags.ProbeData {
		uploadTest.ProbeData = probeData
	}

	rndAssetName := fmt.Sprintf("load_test_import_%s", randName())
	glog.V(logs.DEBUG).Infof("Importing %s from %s", rndAssetName, url)
	asset, task, err := vt.lapi.UploadViaURL(url, rndAssetName, "")

	defer wg.Done()

	if err != nil {
		glog.V(logs.SHORT).Infof("Error importing asset: %v", err)
		uploadTest.ImportSuccess = 0
		uploadTest.ErrorMessage = err.Error()
		vt.writeResultNdjson(uploadTest, startTime)
		return
	} else {
		uploadTest.ImportSuccess = 1
		uploadTest.AssetID = asset.ID
		uploadTest.TaskID = task.ID
	}

	if !vt.cliFlags.KeepAssets {
		defer vt.lapi.DeleteAsset(asset.ID)
	}

	if vt.cliFlags.TaskCheck {
		err := vt.checkTaskProcessing(api.TaskOnlyId{ID: uploadTest.TaskID}, uploadTest.AssetID)
		if err != nil {
			uploadTest.TaskCheckSuccess = 0
			uploadTest.ErrorMessage = err.Error()
		} else {
			uploadTest.TaskCheckSuccess = 1
		}
	}

	uploadTest.EndTime = time.Now().Format("2006-01-02 15:04:05")

	vt.writeResultNdjson(uploadTest, startTime)
}

func (vt *vodLoadTester) writeResultNdjson(uploadTest uploadTest, startTime time.Time) {
	endTime := time.Now()
	elapsed := endTime.Sub(startTime)

	uploadTest.EndTime = endTime.Format("2006-01-02 15:04:05")
	uploadTest.Elapsed = uint(elapsed.Seconds())

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

func (vt *vodLoadTester) checkTaskProcessing(processingTask api.TaskOnlyId, assetID string) error {
	taskPollDuration := time.Duration(vt.cliFlags.TaskCheckPollTime) * time.Second
	startTime := time.Now()
	timeout := time.Duration(vt.cliFlags.TaskCheckTimeout) * time.Second
	for {

		time.Sleep(taskPollDuration)

		task, err := vt.lapi.GetTask(processingTask.ID, false)
		progress := ""

		if err != nil {
			glog.Errorf("Error retrieving task id=%s err=%v", processingTask.ID, err)
			if strings.Contains(err.Error(), "connection reset by peer") || strings.Contains(err.Error(), "520") || strings.Contains(err.Error(), "no such host") {
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

		if task.Status.Phase == "running" {
			percentage := task.Status.Progress * 100
			stringPercentage := strconv.FormatFloat(percentage, 'f', 2, 64)
			progress = fmt.Sprintf("progress=%s%%", stringPercentage)
		}

		glog.V(logs.DEBUG).Infof("Waiting %s for task id=%s to be processed, elapsed=%s %s", taskPollDuration, processingTask.ID, time.Since(startTime), progress)

	}
}

func (vt *vodLoadTester) checkAssetSource(assetID string) error {
	taskPollDuration := time.Duration(vt.cliFlags.TaskCheckPollTime) * time.Second
	startTime := time.Now()
	timeout := time.Duration(vt.cliFlags.TaskCheckTimeout) * time.Second
	for {

		time.Sleep(taskPollDuration)

		asset, err := vt.lapi.GetAsset(assetID, false)

		if err != nil {
			glog.Errorf("Error retrieving asset id=%s err=%v", assetID, err)
			if strings.Contains(err.Error(), "connection reset by peer") || strings.Contains(err.Error(), "520") || strings.Contains(err.Error(), "no such host") {
				// Retry
				glog.V(logs.SHORT).Infof("Livepeer Studio API unreachable, retrying getting task information.... ")
				continue
			}
			return fmt.Errorf("error retrieving asset id=%s: %w", assetID, err)
		}

		if time.Since(startTime) > timeout {
			glog.Errorf("Internal timeout processing asset, asset=%s", assetID)
			return fmt.Errorf("timeout processing asset, asset=%s", assetID)
		}

		if asset.PlaybackURL != "" {
			glog.Infof("Source playback ready, assetId=%s", assetID)
			return nil
		}

		glog.V(logs.DEBUG).Infof("Waiting %s for asset source id=%s to be processed, elapsed=%s", taskPollDuration, assetID, time.Since(startTime))

	}
}

func randName() string {
	x := make([]byte, 10)
	for i := 0; i < len(x); i++ {
		x[i] = byte(rand.Uint32())
	}
	return fmt.Sprintf("%x", x)
}
