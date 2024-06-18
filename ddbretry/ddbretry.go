package ddbretry

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type DynamoDBClient interface {
	DeleteItem(context.Context, *dynamodb.DeleteItemInput, ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
}

type RetryDynamoDBClient struct {
	DynamoDBClient
	Retries     int
	BackOffTime time.Duration
}

func NewRetryDynamoDBClient(client DynamoDBClient, retries int, backOff time.Duration) *RetryDynamoDBClient {
	return &RetryDynamoDBClient{
		DynamoDBClient: client,
		Retries:        retries,
		BackOffTime:    backOff,
	}
}

func (c *RetryDynamoDBClient) DeleteItem(ctx context.Context, input *dynamodb.DeleteItemInput, o ...func(*dynamodb.Options)) (output *dynamodb.DeleteItemOutput, err error) {
	retries := c.Retries
	infinite := retries == -1
	for retries >= 0 || infinite {
		output, err = c.DynamoDBClient.DeleteItem(ctx, input, o...)
		if err != nil {
			if IsProvisionedThroughputExceededException(err) {
				if retries > 0 {
					retries--
					time.Sleep(c.BackOffTime)
				} else if infinite {
					time.Sleep(c.BackOffTime)
				} else {
					return
				}
			} else {
				return
			}
		} else {
			return
		}
	}

	return nil, NewInvalidRetryError(retries)
}

func IsProvisionedThroughputExceededException(err error) bool {
	_, ok := err.(*types.ProvisionedThroughputExceededException)

	return ok
}
