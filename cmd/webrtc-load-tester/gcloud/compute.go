package gcloud

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	konlet "github.com/GoogleCloudPlatform/konlet/gce-containers-startup/types"
	"github.com/golang/glog"
	compute "google.golang.org/api/compute/v1"
	"google.golang.org/api/googleapi"
	"gopkg.in/yaml.v2"
)

var (
	computeClient *compute.Service

	konletRestartPolicyNever = konlet.RestartPolicyNever // just so we can take an address easily
)

type VMTemplateSpec struct {
	ContainerImage string
	Role           string
	Args           []string

	TestID      string
	MachineType string
}

func CreateVMTemplate(ctx context.Context, spec VMTemplateSpec) (url, name string, err error) {
	templateName := fmt.Sprintf("load-tester-%s-%s", spec.TestID[:8], spec.Role)

	containerSpec, err := yaml.Marshal(konlet.ContainerSpec{
		Spec: konlet.ContainerSpecStruct{
			Containers: []konlet.Container{{
				Name:    "load-tester",
				Image:   spec.ContainerImage,
				Command: []string{"webrtc-load-tester"},
				Args:    append([]string{spec.Role}, spec.Args...),
			}},
			RestartPolicy: &konletRestartPolicyNever,
		},
	})
	if err != nil {
		return "", "", fmt.Errorf("error creating VM container spec: %w", err)
	}

	template := &compute.InstanceTemplate{
		Name:        templateName,
		Description: "test-id=" + spec.TestID,
		Properties: &compute.InstanceProperties{
			MachineType: spec.MachineType,
			Disks: []*compute.AttachedDisk{
				{
					Type:       "PERSISTENT",
					Boot:       true,
					AutoDelete: true,
					InitializeParams: &compute.AttachedDiskInitializeParams{
						SourceImage: "projects/cos-cloud/global/images/family/cos-stable",
						DiskSizeGb:  10,
					},
				},
			},
			NetworkInterfaces: []*compute.NetworkInterface{
				{
					Name: "global/networks/default",
					AccessConfigs: []*compute.AccessConfig{
						{
							Name:        "External NAT",
							Type:        "ONE_TO_ONE_NAT",
							NetworkTier: "STANDARD",
						},
					},
				},
			},
			Metadata: &compute.Metadata{
				Items: []*compute.MetadataItems{
					{
						Key:   "gce-container-declaration",
						Value: googleapi.String(string(containerSpec)),
					},
				},
			}},
	}

	op, err := computeClient.InstanceTemplates.Insert(projectID, template).Context(ctx).Do()
	if err != nil {
		return "", "", fmt.Errorf("error creating GCE instance template: %w", err)
	}

	err = waitForOperation(ctx, op)
	if err != nil {
		return "", "", fmt.Errorf("error creating GCE instance template: %w", err)
	}

	template, err = computeClient.InstanceTemplates.Get(projectID, templateName).Context(ctx).Do()
	if err != nil {
		return "", "", fmt.Errorf("error getting GCE instance template: %w", err)
	}

	return template.SelfLink, name, nil
}

func DeleteVMTemplate(templateName string) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	op, err := computeClient.InstanceTemplates.Delete(projectID, templateName).Context(ctx).Do()
	if err != nil {
		glog.Errorf("error deleting GCE instance template: %v", err)
		return
	}

	err = waitForOperation(ctx, op)
	if err != nil {
		glog.Errorf("error deleting GCE instance template: %v", err)
		return
	}
}

func CreateVMGroup(ctx context.Context, spec VMTemplateSpec, templateURL, region string, numInstances int64) (string, error) {
	name := fmt.Sprintf("load-tester-%s-%s-%s", spec.TestID[:8], spec.Role, region)
	instanceGroupManager := &compute.InstanceGroupManager{
		Name:             name,
		Description:      "test-id=" + spec.TestID,
		BaseInstanceName: name,
		InstanceTemplate: templateURL,
		TargetSize:       numInstances,
	}

	op, err := computeClient.RegionInstanceGroupManagers.Insert(projectID, region, instanceGroupManager).Context(ctx).Do()
	if err != nil {
		return "", fmt.Errorf("error creating instance group: %w", err)
	}

	return name, waitForOperation(ctx, op)
}

// DeleteVMGroup deletes a VM group and waits for the operation to complete. It
// doesn't receive a ctx because it's meant to run as a cleanup on shutdown.
func DeleteVMGroup(region, groupName string) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	op, err := computeClient.RegionInstanceGroupManagers.Delete(projectID, region, groupName).Context(ctx).Do()
	if err != nil {
		glog.Errorf("error deleting VM group %s: %v", groupName, err)
		return
	}

	if err = waitForOperation(ctx, op); err != nil {
		glog.Errorf("error deleting VM group %s: %v", groupName, err)
		return
	}
}

type VMGroupInfo struct {
	Region, Name string
}

func DeleteVMGroups(groups []VMGroupInfo) {
	wg := sync.WaitGroup{}
	wg.Add(len(groups))

	for _, group := range groups {
		go func(group VMGroupInfo) {
			defer wg.Done()
			DeleteVMGroup(group.Region, group.Name)
		}(group)
	}
}

func CheckVMGroupStatus(ctx context.Context, region, groupName string) error {
	instances, err := computeClient.RegionInstanceGroupManagers.ListManagedInstances(projectID, region, groupName).Context(ctx).Do()
	if err != nil {
		return fmt.Errorf("error getting VM group instances: %w", err)
	}

	status := map[string]int{}
	running := true
	for _, instance := range instances.ManagedInstances {
		status[instance.InstanceStatus]++
		running = running && instance.InstanceStatus == "RUNNING"
	}

	glog.Infof("VM group %s: running=%v status=%v",
		simpleName(groupName), running, status)

	return nil
}

func ListVMGroups(ctx context.Context, region, testID string) ([]string, error) {
	groups, err := computeClient.RegionInstanceGroupManagers.List(projectID, region).Context(ctx).Do()
	if err != nil {
		return nil, fmt.Errorf("error listing VM groups: %w", err)
	}

	var groupNames []string
	for _, group := range groups.Items {
		if group.Description == "test-id="+testID {
			groupNames = append(groupNames, group.Name)
		}
	}

	return groupNames, nil
}

func ListVMTemplates(ctx context.Context, testID string) ([]string, error) {
	templates, err := computeClient.InstanceTemplates.List(projectID).Context(ctx).Do()
	if err != nil {
		return nil, fmt.Errorf("error listing VM templates: %w", err)
	}

	var templateNames []string
	for _, template := range templates.Items {
		if template.Description == "test-id="+testID {
			templateNames = append(templateNames, template.Name)
		}
	}

	return templateNames, nil
}

func waitForOperation(ctx context.Context, op *compute.Operation) (err error) {
	for {
		var currentOp *compute.Operation
		if op.Region == "" {
			currentOp, err = computeClient.GlobalOperations.Get(projectID, op.Name).Context(ctx).Do()
		} else {
			// op.Region is a fully qualified URL, grab only the last path segment which is the region name
			region := op.Region[strings.LastIndex(op.Region, "/")+1:]
			currentOp, err = computeClient.RegionOperations.Get(projectID, region, op.Name).Context(ctx).Do()
		}
		if err != nil {
			return fmt.Errorf("error getting operation status: %w", err)
		}

		if currentOp.Status == "DONE" {
			if currentOp.Error != nil {
				return fmt.Errorf("operation error: %v", currentOp.Error.Errors)
			}
			return nil
		}

		time.Sleep(3 * time.Second)
	}
}
