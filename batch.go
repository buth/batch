package batch

import (
	"context"
	"sync"
	"time"
)

type Loader[K, V, T any] interface {
	// CanAdd returns whether or not the key can be added to the batch without
	// exceeding its capacity.
	CanAdd(key K) bool

	// Add adds the key to the batch and returns a token that can be used to
	// retrieve the resulting value after Flush is called.
	Add(key K) (token T)

	// Flush is called only once after which no further calls to CanAdd or Add
	// will be made.
	Flush(ctx context.Context) error

	// Load returns the value for the given token and will only be called after
	// Flush if it does not return an error. Load must be safe to call
	// concurrently.
	Load(token T) (value V, err error)
}

type batch[K, V, T any] struct {
	done   chan struct{}
	loader Loader[K, V, T]
	timer  *time.Timer
	wg     sync.WaitGroup
	err    error
}

func (b *batch[K, V, T]) add(ctx context.Context, key K) T {
	b.wg.Add(1)
	go func() {
		<-ctx.Done()
		b.wg.Done()
	}()

	return b.loader.Add(key)
}

func (b *batch[K, V, T]) flush() {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		b.wg.Wait()
		cancel()
	}()

	b.err = b.loader.Flush(ctx)
	close(b.done)
}

func (b *batch[K, V, T]) load(ctx context.Context, token T) (V, error) {
	select {
	case <-ctx.Done():
		var zero V
		return zero, ctx.Err()
	case <-b.done:
		if b.err != nil {
			var zero V
			return zero, b.err
		}

		return b.loader.Load(token)
	}
}

type Batcher[K, V, T any] struct {
	mu        sync.Mutex
	batch     *batch[K, V, T]
	NewLoader func() Loader[K, V, T]
	Duration  time.Duration
}

func (b *Batcher[K, V, T]) setNewBatch() {
	batch := &batch[K, V, T]{
		done:   make(chan struct{}),
		loader: b.NewLoader(),
	}

	batch.timer = time.AfterFunc(b.Duration, func() {
		b.mu.Lock()

		// Remove the reference to the batch if it's still current in order to
		// prevent any further additions.
		if b.batch == batch {
			b.batch = nil
		}

		b.mu.Unlock()

		// Flush the batch outside of the lock.
		batch.flush()
	})

	b.batch = batch
}

func (b *Batcher[K, V, T]) Load(ctx context.Context, key K) (V, error) {
	b.mu.Lock()

	if b.batch == nil {
		b.setNewBatch()
	}

	// If the key cannot be added to the batch, flush the current batch and
	// create a new one.
	if !b.batch.loader.CanAdd(key) {
		if b.batch.timer.Stop() {
			// Flush the batch in a new goroutine.
			go b.batch.flush()
		}

		b.setNewBatch()
	}

	batch := b.batch
	token := batch.add(ctx, key)

	b.mu.Unlock()

	return batch.load(ctx, token)
}
