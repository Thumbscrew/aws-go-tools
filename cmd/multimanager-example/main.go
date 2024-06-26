package main

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/Thumbscrew/aws-go-tools/multimanager"
	"github.com/Thumbscrew/aws-go-tools/s3strings"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Name: "multimanager-example",
		Commands: []*cli.Command{
			{
				Name:        "upload",
				Description: "upload files to an S3 bucket from a local directory",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "bucket",
						Aliases:  []string{"b"},
						Required: true,
						Usage:    "S3 bucket to upload to",
					},
					&cli.StringFlag{
						Name:     "dir",
						Aliases:  []string{"d"},
						Required: true,
						Usage:    "local directory to upload",
					},
				},
				Action: upload,
			},
			{
				Name:        "download",
				Description: "download files from an S3 bucket to a local directory",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "bucket",
						Aliases:  []string{"b"},
						Required: true,
						Usage:    "S3 bucket to download from",
					},
					&cli.StringFlag{
						Name:     "prefix",
						Aliases:  []string{"p"},
						Required: false,
						Usage:    "S3 prefix to download from",
					},
					&cli.StringFlag{
						Name:     "dir",
						Aliases:  []string{"d"},
						Required: true,
						Usage:    "local directory to download to",
					},
				},
				Action: download,
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Print("fatal: ", err)
		os.Exit(1)
	}
}

func upload(ctx *cli.Context) error {
	bucket := ctx.String("b")
	dir := ctx.String("d")

	// collect all files in directory
	var filesToUpload []string
	err := filepath.Walk(dir, func(path string, info fs.FileInfo, err error) error {
		if !info.IsDir() {
			filesToUpload = append(filesToUpload, path)
		}

		return nil
	})
	if err != nil {
		return err
	}

	// instantiate S3 client
	cfg, err := config.LoadDefaultConfig(ctx.Context)
	if err != nil {
		return err
	}
	s3Client := s3.NewFromConfig(cfg)

	inputs := make([]*s3.PutObjectInput, len(filesToUpload))
	files := make([]*os.File, len(filesToUpload))

	// open all files for reading
	for i, file := range filesToUpload {
		files[i], err = os.Open(file)
		if err != nil {
			return err
		}

		inputs[i] = &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(filepath.Base(file)),
			Body:   files[i],
		}
	}

	outputs := multimanager.PutObjects(ctx.Context, s3Client, inputs)

	// close all open file handles
	closeFiles(files)

	outputCount := 0
	errCount := 0
	for _, output := range outputs {
		if output.Output != nil {
			outputCount++
		}
		if output.Err != nil {
			errCount++
		}
	}

	fmt.Printf("there are %d outputs and %d errors", outputCount, errCount)

	return nil
}

func download(ctx *cli.Context) error {
	bucket := ctx.String("b")
	prefix := ctx.String("p")
	dir := ctx.String("d")

	// instantiate S3 client
	cfg, err := config.LoadDefaultConfig(ctx.Context)
	if err != nil {
		return err
	}
	s3Client := s3.NewFromConfig(cfg)

	// list all objects at prefix
	var prefixPtr *string
	if prefix != "" {
		prefixPtr = aws.String(prefix)
	}
	res, err := s3Client.ListObjectsV2(ctx.Context, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: prefixPtr,
	})
	if err != nil {
		return err
	}

	var keysToDownload []string
	var localPaths []string
	for _, object := range res.Contents {
		keysToDownload = append(keysToDownload, *object.Key)
		localPath := filepath.Join(dir, s3strings.RemoveObjectPrefix(*object.Key))
		localPaths = append(localPaths, localPath)
	}

	inputs := make([]multimanager.GetObjectsInput, len(keysToDownload))
	files := make([]*os.File, len(keysToDownload))

	// create all files for writing
	for i, file := range localPaths {
		files[i], err = os.Create(file)
		if err != nil {
			return err
		}

		inputs[i] = multimanager.GetObjectsInput{
			WriterAt: files[i],
			GetObjectInput: &s3.GetObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(keysToDownload[i]),
			},
		}
	}

	outputs := multimanager.GetObjects(ctx.Context, s3Client, inputs)

	// close all open file handles
	closeFiles(files)

	var outputCount int64
	errCount := 0
	for _, output := range outputs {
		if output.Output > -1 {
			outputCount += output.Output
		}
		if output.Err != nil {
			errCount++
		}
	}

	fmt.Printf("%d bytes were written and %d errors occurred", outputCount, errCount)

	return nil
}

func closeFiles(files []*os.File) {
	for _, f := range files {
		f.Close()
	}
}
