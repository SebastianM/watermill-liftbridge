package liftbridge

import (
	"context"
	"fmt"
	"sync"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/liftbridge-io/go-liftbridge/v2"
	"go.uber.org/zap"
)

type Publisher struct {
	client         liftbridge.Client
	messageOptions []liftbridge.MessageOption

	mu            sync.Mutex
	checkedTopics map[string]struct{}
}

var _ message.Publisher = &Publisher{}

func ensureStreamExists(client liftbridge.Client, topic string, checkedTopics map[string]struct{}) error {
	if _, ok := checkedTopics[topic]; ok {
		return nil
	}
	err := client.CreateStream(context.Background(), topic, topic+"-stream")
	if err != liftbridge.ErrStreamExists {
		return err
	}
	checkedTopics[topic] = struct{}{}
	return nil
}

// Close implements message.Publisher
func (p *Publisher) Close() error {
	return nil
}

// Publish implements message.Publisher
func (p *Publisher) Publish(topic string, messages ...*message.Message) error {
	p.mu.Lock()
	if err := ensureStreamExists(p.client, topic, p.checkedTopics); err != nil {
		return err
	}
	p.mu.Unlock()
	for _, m := range messages {
		if _, err := p.client.Publish(context.Background(), topic+"-stream", m.Payload, liftbridge.AckPolicyLeader(), liftbridge.Header("watermillUUID", []byte(m.UUID))); err != nil {
			return err
		}
	}
	return nil
}

func NewPublisher(client liftbridge.Client, messageOptions ...liftbridge.MessageOption) *Publisher {
	return &Publisher{
		client:         client,
		messageOptions: messageOptions,
		checkedTopics:  map[string]struct{}{},
	}
}

type Subscriber struct {
	client  liftbridge.Client
	options []liftbridge.SubscriptionOption

	mu            sync.Mutex
	checkedTopics map[string]struct{}
}

var _ message.Subscriber = &Subscriber{}

func NewSubscriber(client liftbridge.Client, options ...liftbridge.SubscriptionOption) *Subscriber {
	return &Subscriber{
		client:        client,
		checkedTopics: map[string]struct{}{},
		options:       options,
	}
}

// Subscribe implements message.Subscriber
func (s *Subscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	s.mu.Lock()
	if err := ensureStreamExists(s.client, topic, s.checkedTopics); err != nil {
		return nil, err
	}
	s.mu.Unlock()
	c := make(chan *message.Message)
	err := s.client.Subscribe(ctx, topic+"-stream", func(msg *liftbridge.Message, err error) {
		if err != nil {
			close(c)
			return
		}
		c <- message.NewMessage(string(msg.Headers()["watermillUUID"]), msg.Value())
	})
	return c, err
}

func (s *Subscriber) Close() error {
	return nil
}

type ConsumerGroupSubscriber struct {
	client              liftbridge.Client
	subscriptionOptions []liftbridge.SubscriptionOption
	consumerOptions     []liftbridge.ConsumerOption
	groupID             string
	log                 *zap.Logger

	mu            sync.Mutex
	checkedTopics map[string]struct{}
}

var _ message.Subscriber = &ConsumerGroupSubscriber{}

func NewConsumerGroupSubscriber(
	client liftbridge.Client,
	groupID string,
	subscriptionOptions []liftbridge.SubscriptionOption,
	consumerOptions []liftbridge.ConsumerOption,
	log *zap.Logger,
) *ConsumerGroupSubscriber {
	return &ConsumerGroupSubscriber{
		client:              client,
		groupID:             groupID,
		subscriptionOptions: subscriptionOptions,
		consumerOptions:     consumerOptions,
		checkedTopics:       map[string]struct{}{},
		log:                 log.With(zap.String("groupID", groupID)),
	}
}

func (s *ConsumerGroupSubscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	s.mu.Lock()
	if err := ensureStreamExists(s.client, topic, s.checkedTopics); err != nil {
		s.log.Error("Could not ensure that stream exists", zap.Error(err))
		return nil, err
	}
	s.mu.Unlock()
	c := make(chan *message.Message)
	consumer, err := s.client.CreateConsumer(s.groupID, s.consumerOptions...)
	if err != nil {
		s.log.Error("Could not create consumer", zap.Error(err))
		return nil, fmt.Errorf("cannot create liftbridge consumer: %w", err)
	}

	err = consumer.Subscribe(ctx, []string{topic + "-stream"}, func(msg *liftbridge.Message, err error) {
		if err != nil {
			s.log.Error("Got an error inside subscriber", zap.Error(err))
			close(c)
			return
		}
		c <- message.NewMessage(string(msg.Headers()["watermillUUID"]), msg.Value())
	})
	s.log.Info("Error for new Subscriber is", zap.Error(err))
	return c, err
}

func (s *ConsumerGroupSubscriber) Close() error {
	s.log.Info("Closing subscriber")
	return nil
}
