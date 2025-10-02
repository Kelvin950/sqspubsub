package Sqssubpub


import (
	"context"
	

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-aws/sqs"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/aws/aws-sdk-go-v2/aws"
	amazonsqs "github.com/aws/aws-sdk-go-v2/service/sqs"

)

type SqsSubPub struct {
	Subscriber *sqs.Subscriber
	AwsConfig  *aws.Config
	Logger     watermill.LoggerAdapter
	Publisher *sqs.Publisher
	SqsOpt  []func(*amazonsqs.Options)
}

func NewSqsSub(awsconfig *aws.Config ,logger watermill.LoggerAdapter , Opts ... func(*amazonsqs.Options)) (*SqsSubPub, error) {

	
	 SqsSub := &SqsSubPub{
		AwsConfig: awsconfig, 
		Logger: logger,
		SqsOpt: Opts,
	 }

	err := SqsSub.CreateSqsSub() 

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

func(s *SqsSubPub)Publish(topic string , msg []byte)error{

	return s.Publisher.Publish(topic ,message.NewMessage(watermill.NewUUID() , msg) )
}


func(s *SqsSubPub)Close()error{
   
	if(s.Subscriber !=nil){
	err:= s.Subscriber.Close()

	return err 
	}

	if(s.Publisher !=nil){
		err:= s.Publisher.Close()

		return err 
	}

	return nil
}