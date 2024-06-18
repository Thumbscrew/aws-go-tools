package ddbretry

import (
	"context"
	"errors"
	"testing"
	"time"

	ierrors "github.com/Thumbscrew/aws-go-tools/ddbretry/errors"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
)

func TestIsProvisionedThroughputExceededException(t *testing.T) {
	type args struct {
		err error
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "should return true when error is ProvisionedThroughputExceededException",
			args: args{
				err: &types.ProvisionedThroughputExceededException{},
			},
			want: true,
		},
		{
			name: "should return false when error is not ProvisionedThroughputExceededException",
			args: args{
				err: errors.New("foo"),
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, IsProvisionedThroughputExceededException(tt.args.err))
		})
	}
}

type SuccessfulDynamoDBClient struct {
	ThroughputExceededCount int
}

func (c *SuccessfulDynamoDBClient) DeleteItem(ctx context.Context, input *dynamodb.DeleteItemInput, o ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	for c.ThroughputExceededCount > 0 {
		c.ThroughputExceededCount--
		return nil, &types.ProvisionedThroughputExceededException{}
	}

	return &dynamodb.DeleteItemOutput{}, nil
}

type FailingDynamoDBClient struct {
	ThroughputExceededCount int
	Err                     error
}

func (c *FailingDynamoDBClient) DeleteItem(ctx context.Context, input *dynamodb.DeleteItemInput, o ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	for c.ThroughputExceededCount > 0 {
		c.ThroughputExceededCount--
		return nil, &types.ProvisionedThroughputExceededException{}
	}

	return nil, c.Err
}

func TestRetryDynamoDBClient_DeleteItem(t *testing.T) {
	ctx := context.Background()

	type fields struct {
		DynamoDBClient DynamoDBClient
		Retries        int
		BackOffTime    time.Duration
	}
	type args struct {
		ctx   context.Context
		input *dynamodb.DeleteItemInput
		o     []func(*dynamodb.Options)
	}
	tests := []struct {
		name       string
		fields     fields
		args       args
		wantOutput *dynamodb.DeleteItemOutput
		wantErr    error
	}{
		{
			name: "should receive output from successful call in DynamoDBClient",
			fields: fields{
				DynamoDBClient: &SuccessfulDynamoDBClient{},
			},
			args: args{
				ctx:   ctx,
				input: &dynamodb.DeleteItemInput{},
			},
			wantOutput: &dynamodb.DeleteItemOutput{},
			wantErr:    nil,
		},
		{
			name: "should receive error from failed call in DynamoDBClient",
			fields: fields{
				DynamoDBClient: &FailingDynamoDBClient{
					Err: errors.New("foo"),
				},
			},
			args: args{
				ctx:   ctx,
				input: &dynamodb.DeleteItemInput{},
			},
			wantOutput: nil,
			wantErr:    errors.New("foo"),
		},
		{
			name: "should receive output when retries is higher than number of throughput exceptions",
			fields: fields{
				DynamoDBClient: &SuccessfulDynamoDBClient{
					ThroughputExceededCount: 2,
				},
				Retries: 3,
			},
			args: args{
				ctx:   ctx,
				input: &dynamodb.DeleteItemInput{},
			},
			wantOutput: &dynamodb.DeleteItemOutput{},
			wantErr:    nil,
		},
		{
			name: "should receive throughput exception when number of throughput exceptions is higher than retries",
			fields: fields{
				DynamoDBClient: &SuccessfulDynamoDBClient{
					ThroughputExceededCount: 3,
				},
				Retries: 2,
			},
			args: args{
				ctx:   ctx,
				input: &dynamodb.DeleteItemInput{},
			},
			wantOutput: nil,
			wantErr:    &types.ProvisionedThroughputExceededException{},
		},
		{
			name: "should receive error after throughput exceptions when retries is higher",
			fields: fields{
				DynamoDBClient: &FailingDynamoDBClient{
					ThroughputExceededCount: 2,
					Err:                     errors.New("foo"),
				},
				Retries: 3,
			},
			args: args{
				ctx:   ctx,
				input: &dynamodb.DeleteItemInput{},
			},
			wantOutput: nil,
			wantErr:    errors.New("foo"),
		},
		{
			name: "should receive output after throughput exceptions when retries is infinite",
			fields: fields{
				DynamoDBClient: &SuccessfulDynamoDBClient{
					ThroughputExceededCount: 10,
				},
				Retries: -1,
			},
			args: args{
				ctx:   ctx,
				input: &dynamodb.DeleteItemInput{},
			},
			wantOutput: &dynamodb.DeleteItemOutput{},
			wantErr:    nil,
		},
		{
			name: "should receive InvalidRetryError when retries value is invalid",
			fields: fields{
				DynamoDBClient: &SuccessfulDynamoDBClient{
					ThroughputExceededCount: 10,
				},
				Retries: -2,
			},
			args: args{
				ctx:   ctx,
				input: &dynamodb.DeleteItemInput{},
			},
			wantOutput: nil,
			wantErr:    ierrors.NewInvalidRetryError(-2),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &RetryDynamoDBClient{
				DynamoDBClient: tt.fields.DynamoDBClient,
				Retries:        tt.fields.Retries,
				BackOffTime:    tt.fields.BackOffTime,
			}
			gotOutput, err := c.DeleteItem(tt.args.ctx, tt.args.input, tt.args.o...)
			assert.Equal(t, tt.wantOutput, gotOutput)
			assert.Equal(t, tt.wantErr, err)
		})
	}
}
