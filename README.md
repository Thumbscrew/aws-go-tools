# AWS Go Tools
A collection of Go utility packages for use with the AWS Go SDK v2

## Packages

### [multimanager](multimanager)

A multithreaded downloader and uploader that uploads/downloads as many files as you provide at once. Uses the [github.com/aws/aws-sdk-go-v2/feature/s3/manager](https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/feature/s3/manager) package to support multi-part uploads.

See [example](cmd/multimanager-example/main.go) for usage.

### [s3strings](s3strings)

A package containing functions for S3-related string manipulation.

## Contributing

All contributions welcome!

## License

[MIT License](LICENSE)
