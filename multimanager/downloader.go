package multimanager

import (
	"context"
	"io"
	"sync"

	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type DownloaderClient interface {
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

type GetObjectsOutput struct {
	Output int64
	Err    error
}

type GetObjectsInput struct {
	io.WriterAt
	*s3.GetObjectInput
}

func GetObjects(ctx context.Context, c DownloaderClient, inputs []GetObjectsInput, o ...func(*manager.Downloader)) []GetObjectsOutput {
	inputCount := len(inputs)
	outputs := make([]GetObjectsOutput, inputCount)
	outputChan := make(chan *GetObjectsOutput, inputCount)
	defer close(outputChan)

	wg := &sync.WaitGroup{}
	for _, input := range inputs {
		wg.Add(1)

		go download(ctx, &downloadInput{
			Client:     c,
			WriterAt:   input.WriterAt,
			Input:      input.GetObjectInput,
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

type downloadInput struct {
	Client DownloaderClient
	io.WriterAt
	Input *s3.GetObjectInput
	*sync.WaitGroup
	OutputChan chan *GetObjectsOutput
}

func download(ctx context.Context, i *downloadInput, o ...func(*manager.Downloader)) {
	defer i.WaitGroup.Done()

	d := manager.NewDownloader(i.Client, o...)
	n, err := d.Download(ctx, i.WriterAt, i.Input, o...)
	i.OutputChan <- &GetObjectsOutput{
		Output: n,
		Err:    err,
	}
}
