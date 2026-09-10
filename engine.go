package engine

import (
	"context"
	"errors"
	"sync"
	"time"
)

var (
	ErrChannelNotFound = errors.New("channel not found")
	ErrChannelNoCodec  = errors.New("channel codec not ready")
	ErrEngineClosed    = errors.New("engine already shut down")
)

type ChannelConfig struct {
	Id             string
	Name           string
	Url            string
	OnDemand       bool
	RecordEnabled  bool
	SegmentSeconds int
}

type Config struct {
	Context         context.Context
	Log             Logger
	RecordDir       string
	ConnectTimeout  time.Duration
	ReadTimeout     time.Duration
	NoKeyframeAfter time.Duration
}

type RecordEvent struct {
	StreamId string
	FilePath string
	Start    time.Time
	End      time.Time
	Size     int64
	Segments int
}

type Events struct {
	OnChannelStatus func(streamId string, online bool)
	OnRecordClosed  func(ev RecordEvent)
}

type Engine struct {
	ctx      context.Context
	cancel   context.CancelFunc
	mu       sync.RWMutex
	opsMu    sync.Mutex
	channels map[string]*channel
	stopped  bool
	cfg      Config
	events   Events
}

func New(cfg Config, events Events) *Engine {
	if cfg.Context == nil {
		panic("engine: Config.Context is required")
	}
	if cfg.Log == nil {
		panic("engine: Config.Log is required")
	}
	if cfg.NoKeyframeAfter <= 0 {
		cfg.NoKeyframeAfter = 20 * time.Second
	}
	ctx, cancel := context.WithCancel(cfg.Context)
	cfg.Context = ctx
	e := &Engine{
		ctx:      ctx,
		cancel:   cancel,
		channels: make(map[string]*channel),
		cfg:      cfg,
		events:   events,
	}
	context.AfterFunc(ctx, e.Shutdown)
	return e
}

func (e *Engine) ApplyChannel(cfg ChannelConfig) error {
	e.opsMu.Lock()
	defer e.opsMu.Unlock()
	if e.stopped || e.ctx.Err() != nil {
		return ErrEngineClosed
	}
	e.mu.Lock()
	old, exists := e.channels[cfg.Id]
	if exists && old.sameConfig(cfg) {
		e.mu.Unlock()
		return nil
	}
	ch := newChannel(cfg, &e.cfg, &e.events)
	e.channels[cfg.Id] = ch
	e.mu.Unlock()
	if exists {
		old.shutdown()
	}
	ch.start()
	return nil
}

func (e *Engine) RemoveChannel(id string) error {
	e.opsMu.Lock()
	defer e.opsMu.Unlock()
	if e.stopped {
		return ErrChannelNotFound
	}
	e.mu.Lock()
	ch, ok := e.channels[id]
	if ok {
		delete(e.channels, id)
	}
	e.mu.Unlock()
	if !ok {
		return ErrChannelNotFound
	}
	ch.shutdown()
	return nil
}

func (e *Engine) SetSegment(id string, seconds int) error {
	e.opsMu.Lock()
	defer e.opsMu.Unlock()
	e.mu.RLock()
	ch, ok := e.channels[id]
	e.mu.RUnlock()
	if !ok {
		return ErrChannelNotFound
	}
	ch.updateSegment(seconds)
	return nil
}

func (e *Engine) SetRecord(id string, on bool) error {
	e.opsMu.Lock()
	defer e.opsMu.Unlock()
	e.mu.RLock()
	ch, ok := e.channels[id]
	e.mu.RUnlock()
	if !ok {
		return ErrChannelNotFound
	}
	ch.toggleRecorder(on)
	return nil
}

func (e *Engine) Snapshot(id string) (online bool, recording bool) {
	e.mu.RLock()
	ch, ok := e.channels[id]
	e.mu.RUnlock()
	if !ok {
		return false, false
	}
	return ch.snapshot()
}

func (e *Engine) Shutdown() {
	e.cancel()
	e.opsMu.Lock()
	if e.stopped {
		e.opsMu.Unlock()
		return
	}
	e.stopped = true
	e.mu.Lock()
	chans := make([]*channel, 0, len(e.channels))
	for _, ch := range e.channels {
		chans = append(chans, ch)
	}
	e.channels = make(map[string]*channel)
	e.mu.Unlock()
	e.opsMu.Unlock()
	var wg sync.WaitGroup
	for _, ch := range chans {
		wg.Add(1)
		go func(c *channel) {
			defer wg.Done()
			c.shutdown()
		}(ch)
	}
	wg.Wait()
}

func (e *Engine) RunAll(fn func() ([]ChannelConfig, error)) error {
	list, err := fn()
	if err != nil {
		return err
	}
	for _, cfg := range list {
		if err := e.ApplyChannel(cfg); err != nil {
			e.cfg.Log.Warnf("[engine] apply channel %s: %v", cfg.Id, err)
		}
	}
	return nil
}
