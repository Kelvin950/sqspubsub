package Sqssubpub


import (
	"context"
	"net/url"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-aws/sqs"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/aws/aws-sdk-go-v2/aws"
	amazonsqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	transport "github.com/aws/smithy-go/endpoints"
)

type SqsSubPub struct {
	Subscriber *sqs.Subscriber
	AwsConfig  *aws.Config
	Logger     watermill.LoggerAdapter
	Publisher *sqs.Publisher
	SqsOpt  []func(*amazonsqs.Options)
}

func NewSqsSub(awsconfig *aws.Config ,logger watermill.LoggerAdapter) (*SqsSubPub, error) {

	uri, err := url.Parse("http://localstack:4566")
	if err != nil {
		return nil, err
	}
	 SqsSub := &SqsSubPub{
		AwsConfig: awsconfig, 
		Logger: logger,
		SqsOpt: []func(*amazonsqs.Options){
		amazonsqs.WithEndpointResolverV2(sqs.OverrideEndpointResolver{
			Endpoint: transport.Endpoint{
				URI: *uri,
			},
		}),
	},
	 }

	err = SqsSub.CreateSqsSub() 

	 return SqsSub ,err
}

func (s *SqsSubPub) CreateSqsSub() error {

	subscriberConfig := sqs.SubscriberConfig{
		AWSConfig: *s.AwsConfig,
		OptFns:    s.SqsOpt,
	}

	subscriber, err := sqs.NewSubscriber(subscriberConfig, s.Logger)
	if err != nil {
		return err
	}

	s.Subscriber = subscriber
	return nil
}

func (s *SqsSubPub) SetSubToTopic(topic string) (<-chan *message.Message, error) {

	messages, err := s.Subscriber.Subscribe(context.Background(), topic)
	if err != nil {
		return nil, err
	}

	return messages, nil

}

func(s *SqsSubPub)CreatePub()error{
		publisherConfig := sqs.PublisherConfig{
		AWSConfig: aws.Config{
			Credentials: aws.AnonymousCredentials{},
		},
		OptFns: s.SqsOpt,
	}

	publisher, err := sqs.NewPublisher(publisherConfig, s.Logger)
	if err != nil {
		return err 
	}

	s.Publisher = publisher 

	return nil
}
