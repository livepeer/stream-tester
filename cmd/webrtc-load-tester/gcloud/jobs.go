package gcloud

import (
	"context"
	"fmt"
	"strings"
	"time"

	run "cloud.google.com/go/run/apiv2"
	"cloud.google.com/go/run/apiv2/runpb"
	"github.com/golang/glog"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/types/known/durationpb"
)

var (
	jobsClient       *run.JobsClient
	executionsClient *run.ExecutionsClient
	projectID        string
)

func InitClients(ctx context.Context, credentialsJSON, credsProjectID string) (err error) {
	credsOpt := option.WithCredentialsJSON([]byte(credentialsJSON))
	jobsClient, err = run.NewJobsClient(ctx, credsOpt)
	if err != nil {
		return err
	}

	executionsClient, err = run.NewExecutionsClient(ctx, credsOpt)
	if err != nil {
		return err
	}

	projectID = credsProjectID

	return nil
}

type JobSpec struct {
	Region string

	ContainerImage string
	Role           string
	Args           []string
	Timeout        time.Duration

	TestID          string
	NumTasks        int
	CPUs, MemoryMiB int
}

func CreateJob(ctx context.Context, spec JobSpec) (job *runpb.Job, exec *runpb.Execution, err error) {
	jobName := fmt.Sprintf("webrtc-load-tester-%s-%s-%s", spec.TestID[:8], spec.Role, spec.Region)
	labels := map[string]string{
		"webrtc-load-tester": "true",
		"load-test-id":       spec.TestID,
	}
	glog.Infof("Creating job name=%s cpu=%d memory=%d tasks=%d", jobName, spec.CPUs, spec.MemoryMiB, spec.NumTasks)

	parent := fmt.Sprintf("projects/%s/locations/%s", projectID, spec.Region)
	createOp, err := jobsClient.CreateJob(ctx, &runpb.CreateJobRequest{
		Parent: parent,
		JobId:  jobName,
		Job: &runpb.Job{
			Labels: labels,
			Template: &runpb.ExecutionTemplate{
				Labels:    labels,
				TaskCount: int32(spec.NumTasks),
				Template: &runpb.TaskTemplate{
					Containers: []*runpb.Container{
						{
							Name:  jobName,
							Image: spec.ContainerImage,
							Args:  append([]string{spec.Role}, spec.Args...),
							Resources: &runpb.ResourceRequirements{
								Limits: map[string]string{
									"cpu":    fmt.Sprintf("%d", spec.CPUs),
									"memory": fmt.Sprintf("%dMi", spec.MemoryMiB),
								},
							},
						},
					},
					Timeout: durationpb.New(spec.Timeout),
					Retries: &runpb.TaskTemplate_MaxRetries{MaxRetries: 0},
				},
			},
		},
	})
	if err != nil {
		return nil, nil, fmt.Errorf("error creating job: %w", err)
	}

	job, err = createOp.Wait(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("error waiting for job creation: %w", err)
	}

	// TODO: Could separate this call so orchestrator can re-use job for multiple runs
	runOp, err := jobsClient.RunJob(ctx, &runpb.RunJobRequest{Name: job.Name})
	if err != nil {
		return nil, nil, fmt.Errorf("error running job: %w", err)
	}

	exec, err = runOp.Metadata()
	if err != nil {
		return nil, nil, fmt.Errorf("error getting job execution: %w", err)
	}

	return job, exec, nil
}

// DeleteJob is meant to in background/defer so it doesn't get a ctx and doesn't return an error
func DeleteJob(region, name string) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	fullJobName := fmt.Sprintf("projects/%s/locations/%s/jobs/%s", projectID, region, name)

	it := executionsClient.ListExecutions(ctx, &runpb.ListExecutionsRequest{
		Parent:   fullJobName,
		PageSize: 1000,
	})

	for {
		exec, err := it.Next()
		if err == iterator.Done {
			break
		} else if err != nil {
			glog.Errorf("Error listing executions: %v\n", err)
			return
		}

		glog.Infof("Deleting execution: %s", simpleName(exec.Name))
		deleteOp, err := executionsClient.DeleteExecution(ctx, &runpb.DeleteExecutionRequest{Name: exec.Name})
		if err != nil {
			glog.Errorf("Error deleting execution %s: %v\n", exec.Name, err)
			return
		}

		_, err = deleteOp.Wait(ctx)
		if err != nil {
			glog.Errorf("Error waiting for execution deletion: %v\n", err)
			return
		}
	}

	glog.Infof("Deleting job: %s", simpleName(name))
	deleteOp, err := jobsClient.DeleteJob(ctx, &runpb.DeleteJobRequest{Name: fullJobName})
	if err != nil {
		glog.Errorf("Error deleting job %s: %v\n", name, err)
		return
	}

	_, err = deleteOp.Wait(ctx)
	if err != nil {
		glog.Errorf("Error waiting for job deletion: %v\n", err)
		return
	}
}

func CheckExecutionStatus(ctx context.Context, name string) (finished bool) {
	exec, err := executionsClient.GetExecution(ctx, &runpb.GetExecutionRequest{Name: name})
	if err != nil {
		glog.Errorf("Error getting execution: %w", err)
		return false
	}

	finished = exec.CompletionTime.IsValid()

	completionMsg := ""
	if finished {
		completionMsg = fmt.Sprintf(" (completed at %s)", exec.CompletionTime.AsTime().Format(time.RFC3339))
	}

	glog.Infof("Execution %s%s: running=%d succeeded=%d failed=%d cancelled=%d retried=%d", simpleName(name), completionMsg,
		exec.RunningCount, exec.SucceededCount, exec.FailedCount, exec.CancelledCount, exec.RetriedCount)

	return finished
}

func ListExecutions(ctx context.Context, region, testID string) ([]*runpb.Execution, error) {
	it := executionsClient.ListExecutions(ctx, &runpb.ListExecutionsRequest{
		Parent:   fmt.Sprintf("projects/%s/locations/%s/jobs/-", projectID, region),
		PageSize: 1000,
	})

	var executions []*runpb.Execution
	for {
		exec, err := it.Next()
		if err == iterator.Done {
			break
		} else if err != nil {
			return nil, err
		}

		if exec.Labels["load-test-id"] == testID {
			executions = append(executions, exec)
		}
	}

	return executions, nil
}

func simpleName(name string) string {
	if parts := strings.Split(name, "/"); len(parts) > 1 {
		// trim the fully qualified name prefix from GCloud
		return parts[len(parts)-1]
	}
	return name
}
