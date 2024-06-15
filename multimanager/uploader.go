package multimanager

import (
	"context"
	"sync"

	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type UploaderClient interface {
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	CreateMultipartUpload(context.Context, *s3.CreateMultipartUploadInput, ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error)
	AbortMultipartUpload(context.Context, *s3.AbortMultipartUploadInput, ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error)
	CompleteMultipartUpload(context.Context, *s3.CompleteMultipartUploadInput, ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error)
	UploadPart(context.Context, *s3.UploadPartInput, ...func(*s3.Options)) (*s3.UploadPartOutput, error)
}

type PutObjectsOutput struct {
	Output *manager.UploadOutput
	Err    error
}

func PutObjects(ctx context.Context, c UploaderClient, inputs []*s3.PutObjectInput, o ...func(*manager.Uploader)) []PutObjectsOutput {
	inputCount := len(inputs)
	outputs := make([]PutObjectsOutput, inputCount)
	outputChan := make(chan *PutObjectsOutput, inputCount)
	defer close(outputChan)

	wg := &sync.WaitGroup{}
	for _, input := range inputs {
		wg.Add(1)

		go upload(ctx, &uploadInput{
			Client:     c,
			Input:      input,
			WaitGroup:  wg,
			OutputChan: outputChan,
		})
	}

	wg.Wait()

	for i := 0; i < inputCount; i++ {
		outputs[i] = *<-outputChan
	}

	return outputs
}

type uploadInput struct {
	Client UploaderClient
	Input  *s3.PutObjectInput
	*sync.WaitGroup
	OutputChan chan *PutObjectsOutput
}

func upload(ctx context.Context, i *uploadInput, o ...func(*manager.Uploader)) {
	defer i.WaitGroup.Done()

	u := manager.NewUploader(i.Client, o...)
	resp, err := u.Upload(ctx, i.Input, o...)
	i.OutputChan <- &PutObjectsOutput{
		Output: resp,
		Err:    err,
	}
}
