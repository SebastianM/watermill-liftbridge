package liftbridge

import (
	"context"
	"sync"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/liftbridge-io/go-liftbridge/v2"
)

type Publisher struct {
	client         liftbridge.Client
	messageOptions []liftbridge.MessageOption

	mu            sync.Mutex
	checkedTopics map[string]struct{}
}

func (p *Publisher) ensureStreamExists(topic string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.checkedTopics[topic]; ok {
		return nil
	}
	err := p.client.CreateStream(context.Background(), topic, topic+"-stream")
	if err != liftbridge.ErrStreamExists {
		return err
	}
	p.checkedTopics[topic] = struct{}{}
	return nil
}

// Close implements message.Publisher
func (p *Publisher) Close() error {
	return nil
}

// Publish implements message.Publisher
func (p *Publisher) Publish(topic string, messages ...*message.Message) error {
	if err := p.ensureStreamExists(topic); err != nil {
		return err
	}
	for _, m := range messages {
		if _, err := p.client.Publish(context.Background(), topic+"-stream", m.Payload, liftbridge.AckPolicyLeader(), liftbridge.Header("watermillUUID", []byte(m.UUID))); err != nil {
			return err
		}
	}
	return nil
}

var _ message.Publisher = &Publisher{}
var _ message.Subscriber = &Subscriber{}

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

// Subscribe implements message.Subscriber
func (s *Subscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	if err := s.ensureStreamExists(topic); err != nil {
		return nil, err
	}
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

func (s *Subscriber) ensureStreamExists(topic string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.checkedTopics[topic]; ok {
		return nil
	}
	err := s.client.CreateStream(context.Background(), topic, topic+"-stream")
	if err != liftbridge.ErrStreamExists {
		return err
	}
	s.checkedTopics[topic] = struct{}{}
	return nil
}

func NewSubscriber(client liftbridge.Client, options ...liftbridge.SubscriptionOption) *Subscriber {
	return &Subscriber{
		client:        client,
		checkedTopics: map[string]struct{}{},
		options:       options,
	}
}
