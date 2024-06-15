package main

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/Thumbscrew/aws-go-tools/multimanager"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Name: "multimanager-example",
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
		Action: func(ctx *cli.Context) error {
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
		},
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Print("fatal: ", err)
		os.Exit(1)
	}
}

func closeFiles(files []*os.File) {
	for _, f := range files {
		f.Close()
	}
}
