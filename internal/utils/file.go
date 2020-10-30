package utils

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/livepeer/stream-tester/internal/utils/uhttp"
)

// ErrNotFound not found
var ErrNotFound = errors.New("Not Found")

var httpClient = &http.Client{
	Timeout: 10 * 60 * time.Second,
}

// GetFile download fileName is HTTP url
func GetFile(fileName string) (string, error) {
	if pu, err := url.Parse(fileName); err == nil && (pu.Scheme == "http" || pu.Scheme == "https") {
		start := time.Now()
		fmt.Printf("Downloading file %s\n", fileName)
		resp, err := httpClient.Do(uhttp.GetRequest(fileName))
		if err != nil {
			return "", err
		}
		if resp.StatusCode != http.StatusOK {
			ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			return "", errors.New(resp.Status)
		}
		tempDir, err := ioutil.TempDir("", "loadtester")
		if err != nil {
			return "", err
		}
		_, name := filepath.Split(pu.Path)
		fullFileName := filepath.Join(tempDir, name)
		f, err := os.Create(fullFileName)
		if err != nil {
			return "", err
		}
		if _, err = io.Copy(f, resp.Body); err != nil {
			return "", err
		}
		f.Close()
		resp.Body.Close()
		fmt.Printf("File %s downloaded in %s\n", fileName, time.Since(start))
		return fullFileName, nil
	}
	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		return "", ErrNotFound
		// fmt.Printf("File '%s' does not exists\n", *fileArg)
		// os.Exit(1)
	}
	return fileName, nil
}
