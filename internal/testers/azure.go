package testers

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/golang/glog"
	"github.com/livepeer/joy4/jerrors"
	"github.com/livepeer/stream-tester/model"
)

var azure *azblob.ContainerURL
var azureContainer string

// AzureInit inits azure
func AzureInit(azureStorageAccount, azureAccessKey, _azureContainer string) error {
	if azureStorageAccount == "" && azureAccessKey == "" && _azureContainer == "" {
		return nil
	}
	if azureStorageAccount == "" || azureAccessKey == "" || _azureContainer == "" {
		return errors.New("Should specify all of azure-storage-account, azure-access-key, azure-container")
	}
	azureContainer = _azureContainer

	// Create a default request pipeline using your storage account name and account key.
	credential, err := azblob.NewSharedKeyCredential(azureStorageAccount, azureAccessKey)
	if err != nil {
		return fmt.Errorf("Invalid credentials with error: " + err.Error())
	}
	p := azblob.NewPipeline(credential, azblob.PipelineOptions{})

	// From the Azure portal, get your storage account blob service URL endpoint.
	URL, _ := url.Parse(fmt.Sprintf("https://%s.blob.core.windows.net/%s", azureStorageAccount, _azureContainer))

	// Create a ContainerURL object that wraps the container URL and a request
	// pipeline to make requests.
	au := azblob.NewContainerURL(*URL, p)
	azure = &au
	return nil
}

func save2Azure(fileName string, data []byte) (string, error) {
	var body io.ReadSeeker = bytes.NewReader(data)
	ctx := context.Background() // This example uses a never-expiring context
	blobURL := azure.NewBlobURL(fileName)
	var hdrs azblob.BlobHTTPHeaders
	var ac azblob.BlobAccessConditions
	resp, err := blobURL.ToBlockBlobURL().Upload(ctx, body, hdrs, azblob.Metadata{}, ac)
	glog.Infof("Response for uploading to Azure: %v", resp.Status())
	return azureContainer + "/" + fileName, err
}

func shouldSave(verr error) bool {
	return (azure != nil || Bucket != "" || model.FailHardOnBadSegments) && !(IgnoreNoCodecError && (errors.Is(verr, jerrors.ErrNoAudioInfoFound) || errors.Is(verr, jerrors.ErrNoVideoInfoFound)))
}

func saveToExternalStorage(fileName string, data []byte) (string, string, error) {
	if azure == nil && Bucket == "" {
		return "", "", nil
	}
	if azure != nil {
		extURL, err := save2Azure(fileName, data)
		return extURL, "Azure", err
	}
	extURL, err := save2GS(fileName, data)
	return extURL, "Google Storage", err
}
