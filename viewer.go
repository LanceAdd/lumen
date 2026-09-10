package engine

import "context"

type Subscription struct {
	Frames <-chan *Frame
	Done   <-chan struct{}
	Cancel func()
}

func (e *Engine) TrackInfoContext(ctx context.Context, id string) (*TrackSpec, error) {
	e.mu.RLock()
	ch, ok := e.channels[id]
	e.mu.RUnlock()
	if !ok {
		return nil, ErrChannelNotFound
	}
	return ch.trackWait(ctx)
}

func (e *Engine) Subscribe(id string) (*Subscription, error) {
	e.mu.RLock()
	ch, ok := e.channels[id]
	e.mu.RUnlock()
	if !ok {
		return nil, ErrChannelNotFound
	}
	s, err := ch.hub.addViewer()
	if err != nil {
		return nil, err
	}
	return s.subscribe(), nil
}
