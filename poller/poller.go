package poller

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

type GetterFunc[T any] func(context.Context) (T, error)

type Poller[T any] struct {
	Getter GetterFunc[T]

	updatePeriod time.Duration
	once         sync.Once

	resource      atomic.Pointer[T]
	firstPollDone chan struct{}
}

func NewPoller[T any](getterFunc GetterFunc[T], opts ...Opt[T]) (*Poller[T], error) {
	poller := &Poller[T]{
		Getter:       getterFunc,
		updatePeriod: time.Second,
	}
	poller.resource.Store(new(T))

	for _, opt := range opts {
		if err := opt(poller); err != nil {
			return nil, fmt.Errorf("cannot apply option to poller: %w", err)
		}
	}

	return poller, nil
}

func (p *Poller[T]) GetResource() T {
	return *p.resource.Load()
}

func (p *Poller[T]) Poll(ctx context.Context) error {
	resource, err := p.Getter(ctx)
	if err != nil {
		return fmt.Errorf("cannot poll resource: %w", err)
	}
	p.resource.Store(&resource)
	return nil
}

func (p *Poller[T]) StartPolling(ctx context.Context) {
	p.once.Do(func() {
		var once sync.Once
		ticker := time.NewTicker(p.updatePeriod)
		defer ticker.Stop()

		slog.Info("start polling")

		for {
			err := p.Poll(ctx)
			if err != nil {
				slog.Error("cannot update resource " + err.Error())
			} else {
				if p.firstPollDone != nil {
					once.Do(func() { close(p.firstPollDone) })
				}
				slog.Info("update resource")
			}
			select {
			case <-ticker.C:
			case <-ctx.Done():
				if p.firstPollDone != nil {
					once.Do(func() { close(p.firstPollDone) })
				}
				slog.Error("stop polling")
				return
			}
		}
	})
}

type Opt[T any] func(poller *Poller[T]) error

func WithUpdatePeriod[T any](period time.Duration) Opt[T] {
	return func(poller *Poller[T]) error {
		poller.updatePeriod = period
		return nil
	}
}

// WithFirstPollDone - закрывается после первого успешного полла и не надо ждать из него значения
func WithFirstPollDone[T any](firstPollDone chan struct{}) Opt[T] {
	return func(poller *Poller[T]) error {
		poller.firstPollDone = firstPollDone
		return nil
	}
}
