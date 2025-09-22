# SQS PubSub

A Go library that provides a simple abstraction over [Watermill SQS](https://github.com/ThreeDotsLabs/watermill-aws) for AWS SQS communication. This library is designed to facilitate communication between main applications and jobs running on spot instances, with built-in support for local development using LocalStack.

## Features

- Simple publisher/subscriber interface for AWS SQS
- Built-in LocalStack support for local development
- Abstraction over Watermill SQS for easier integration
- Configurable AWS SQS options

## Installation

```bash
go get github.com/kelvin950/sqspubsub
```

## Prerequisites

- Go 1.24.4 or later
- AWS credentials configured (or LocalStack for local development)
- LocalStack running on `http://localstack:4566` for local development

## Usage

### Basic Setup

```go
package main

import (
    "log"
    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/ThreeDotsLabs/watermill"
    "github.com/kelvin950/sqspubsub"
)

func main() {
    // Create AWS config
    awsConfig := &aws.Config{
        Region: "us-east-1",
        // Add your AWS credentials here
    }
    
    // Create logger
    logger := watermill.NewStdLogger(false, false)
    
    // Create SQS subscriber/publisher
    sqsSub, err := NewSqsSub(awsConfig, logger)
    if err != nil {
        log.Fatal(err)
    }
    
    // Create publisher
    err = sqsSub.CreatePub()
    if err != nil {
        log.Fatal(err)
    }
}
```

### Subscribing to Messages

```go
// Subscribe to a topic
messages, err := sqsSub.SetSubToTopic("your-topic-name")
if err != nil {
    log.Fatal(err)
}

// Process messages
for msg := range messages {
    log.Printf("Received message: %s", string(msg.Payload))
    // Process your message here
    msg.Ack() // Acknowledge the message
}
```

### Publishing Messages

```go
// Create a message
msg := message.NewMessage(watermill.NewUUID(), []byte("Hello, SQS!"))

// Publish to a topic
err := sqsSub.Publisher.Publish("your-topic-name", msg)
if err != nil {
    log.Fatal(err)
}
```

## API Reference

### SqsSub

The main struct that encapsulates SQS functionality.

```go
type SqsSub struct {
    Subscriber *sqs.Subscriber
    AwsConfig  *aws.Config
    Logger     watermill.LoggerAdapter
    Publisher  *sqs.Publisher
    SqsOpt     []func(*amazonsqs.Options)
}
```

### Methods

#### `NewSqsSub(awsconfig *aws.Config, logger watermill.LoggerAdapter) (*SqsSub, error)`

Creates a new SqsSub instance with the provided AWS config and logger. Automatically configures LocalStack endpoint for local development.

#### `CreateSqsSub() error`

Initializes the SQS subscriber with the configured options.

#### `SetSubToTopic(topic string) (<-chan *message.Message, error)`

Subscribes to the specified topic and returns a channel of messages.

**Note**: Currently hardcoded to subscribe to "example-topic". This should be updated to use the provided topic parameter.

#### `CreatePub() error`

Creates and configures the SQS publisher.

## Local Development with LocalStack

This library is pre-configured to work with LocalStack for local development. Ensure LocalStack is running on `http://localstack:4566`.

### Docker Compose Example

```yaml
version: '3.8'
services:
  localstack:
    image: localstack/localstack
    ports:
      - "4566:4566"
    environment:
      - SERVICES=sqs
      - DEBUG=1
    volumes:
      - "/var/run/docker.sock:/var/run/docker.sock"
```

## Dependencies

- [Watermill](https://github.com/ThreeDotsLabs/watermill) - Event streaming library
- [Watermill AWS](https://github.com/ThreeDotsLabs/watermill-aws) - AWS SQS support for Watermill
- [AWS SDK for Go v2](https://github.com/aws/aws-sdk-go-v2) - Official AWS SDK
