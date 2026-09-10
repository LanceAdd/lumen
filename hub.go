package engine

import "sync"

type hub struct {
	mu      sync.RWMutex
	viewers map[*subscriber]struct{}
	closed  bool
}

type subscriber struct {
	hub    *hub
	frames chan *Frame
	done   chan struct{}
	once   sync.Once
}

func newHub() *hub {
	return &hub{viewers: make(map[*subscriber]struct{})}
}

func (h *hub) addViewer() (*subscriber, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil, ErrChannelNotFound
	}
	s := &subscriber{
		hub:    h,
		frames: make(chan *Frame, 800),
		done:   make(chan struct{}),
	}
	h.viewers[s] = struct{}{}
	return s, nil
}

func (s *subscriber) close() {
	s.once.Do(func() { close(s.done) })
}

func (h *hub) remove(s *subscriber) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.viewers[s]; ok {
		delete(h.viewers, s)
		s.close()
	}
}

func (h *hub) closeAll() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.closed = true
	for s := range h.viewers {
		delete(h.viewers, s)
		s.close()
	}
}

func (h *hub) fanout(f *Frame) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for s := range h.viewers {
		select {
		case s.frames <- f:
		default:
		}
	}
}

func (s *subscriber) subscribe() *Subscription {
	return &Subscription{
		Frames: s.frames,
		Done:   s.done,
		Cancel: func() { s.hub.remove(s) },
	}
}
