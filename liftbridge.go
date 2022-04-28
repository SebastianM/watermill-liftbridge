package liftbridge

import (
	"context"
	"sync"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/liftbridge-io/go-liftbridge/v2"
)

type PubSub struct {
	client liftbridge.Client

	mu            sync.Mutex
	checkedTopics map[string]struct{}
}

// Subscribe implements message.Subscriber
func (p *PubSub) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	if err := p.ensureStreamExists(topic); err != nil {
		return nil, err
	}
	c := make(<-chan *message.Message)
	err := p.client.Subscribe(context.Background(), topic+"-stream", func(msg *liftbridge.Message, err error) {
		message.NewMessage(string(msg.Headers()["watermillUUID"]), msg.Value())
	})
	return c, err
}

func (p *PubSub) ensureStreamExists(topic string) error {
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
func (p *PubSub) Close() error {
	return p.client.Close()
}

// Publish implements message.Publisher
func (p *PubSub) Publish(topic string, messages ...*message.Message) error {
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

var _ message.Publisher = &PubSub{}
var _ message.Subscriber = &PubSub{}

func NewPubSub(client liftbridge.Client) *PubSub {
	return &PubSub{
		client:        client,
		checkedTopics: map[string]struct{}{},
	}
}
