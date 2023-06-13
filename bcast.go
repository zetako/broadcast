// Package broadcast implements multi-listener broadcast channels with generic type T.
// See https://pkg.go.dev/github.com/textileio/go-threads/broadcast for non-generic type implementation.
// See https://godoc.org/github.com/tjgq/broadcast for original implementation.
//
// To create an un-buffered broadcast channel, just declare a Broadcaster:
//
//	var b broadcast.Broadcaster[string]
//
// To create a buffered broadcast channel with capacity n, call New:
//
//	b := broadcast.New[string](n)
//
// To add a listener to a channel, call Listen and read from Channel():
//
//	l := b.Listen()
//	for v := range l.Channel() {
//	    // ...
//	}
//
// To send to the channel, call Send:
//
//	b.Send("Hello world!")
//	v <- l.Channel() // returns "Hello world!"
//
// To remove a listener, call Discard.
//
//	l.Discard()
//
// To close the broadcast channel, call Discard. Any existing or future listeners
// will read from a closed channel:
//
//	b.Discard()
//	v, ok <- l.Channel() // returns ok == false
package broadcast

import (
	"errors"
	"fmt"
	"github.com/hashicorp/go-multierror"
	"sync"
	"time"
)

// ErrClosedChannel means the caller attempted to send to one or more closed broadcast channels.
const ErrClosedChannel = broadcastError("send after close")

type broadcastError string

func (e broadcastError) Error() string { return string(e) }

// Broadcaster implements a Publisher. The zero value is a usable un-buffered channel.
type Broadcaster[T any] struct {
	m         sync.Mutex
	listeners map[uint]chan<- T // lazy init
	nextID    uint
	capacity  int
	closed    bool
}

// NewBroadcaster returns a new Broadcaster with the given capacity (0 means un-buffered).
func NewBroadcaster[T any](n int) *Broadcaster[T] {
	return &Broadcaster[T]{capacity: n}
}

// SendWithTimeout broadcasts a message to each listener's channel.
// Sending on a closed channel causes a runtime panic.
// This method blocks for a duration of up to `timeout` on each channel.
// Returns error(s) if it is unable to send on a given listener's channel within `timeout` duration.
func (b *Broadcaster[T]) SendWithTimeout(v T, timeout time.Duration) error {
	b.m.Lock()
	defer b.m.Unlock()
	if b.closed {
		return ErrClosedChannel
	}
	var result *multierror.Error
	for id, l := range b.listeners {
		select {
		case l <- v:
			// Success!
		case <-time.After(timeout):
			err := fmt.Sprintf("unable to send to listener '%d'", id)
			result = multierror.Append(result, errors.New(err))
		}
	}
	if result != nil {
		return result.ErrorOrNil()
	} else {
		return nil
	}
}

// Send broadcasts a message to each listener's channel.
// Sending on a closed channel causes a runtime panic.
// This method is non-blocking, and will return errors if unable to send on a given listener's channel.
func (b *Broadcaster[T]) Send(v T) error {
	return b.SendWithTimeout(v, 0)
}

// Discard closes the channel, disabling the sending of further messages.
func (b *Broadcaster[T]) Discard() {
	b.m.Lock()
	defer b.m.Unlock()
	if b.closed {
		return
	}
	b.closed = true
	for _, l := range b.listeners {
		close(l)
	}
}

// Listen returns a Listener for the broadcast channel.
func (b *Broadcaster[T]) Listen() *Listener[T] {
	b.m.Lock()
	defer b.m.Unlock()
	if b.listeners == nil {
		b.listeners = make(map[uint]chan<- T)
	}
	if b.listeners[b.nextID] != nil {
		b.nextID++
	}
	ch := make(chan T, b.capacity)
	if b.closed {
		close(ch)
	}
	b.listeners[b.nextID] = ch
	return &Listener[T]{ch, b, b.nextID}
}

// Listener implements a Subscriber to broadcast channel.
type Listener[T any] struct {
	ch <-chan T
	b  *Broadcaster[T]
	id uint
}

// Discard closes the Listener, disabling the reception of further messages.
func (l *Listener[T]) Discard() {
	l.b.m.Lock()
	defer l.b.m.Unlock()
	delete(l.b.listeners, l.id)
}

// Channel returns the channel that receives broadcast messages.
func (l *Listener[T]) Channel() <-chan T {
	return l.ch
}
