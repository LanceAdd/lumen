package engine

import (
	"context"
	"sync"
)

type TrackSpec struct {
	Video *H264Spec
	Audio *AACSpec
}

type H264Spec struct {
	SPS []byte
	PPS []byte
}

type AACSpec struct {
	SampleRate   int
	ChannelCount int
}

type channel struct {
	cfg   ChannelConfig
	cfgMu sync.RWMutex

	env *Config
	ev  *Events

	ctx    context.Context
	cancel context.CancelFunc
	once   sync.Once

	mu     sync.RWMutex
	online bool
	info   TrackSpec
	infoNt chan struct{}

	rec *recorderHLS
	run *puller
	hub *hub
}

func newChannel(cfg ChannelConfig, env *Config, ev *Events) *channel {
	ctx, cancel := context.WithCancel(env.Context)
	return &channel{
		cfg:    cfg,
		env:    env,
		ev:     ev,
		ctx:    ctx,
		cancel: cancel,
		infoNt: make(chan struct{}, 1),
		hub:    newHub(),
	}
}

func (c *channel) sameConfig(cfg ChannelConfig) bool {
	c.cfgMu.RLock()
	defer c.cfgMu.RUnlock()
	return c.cfg.Url == cfg.Url &&
		c.cfg.OnDemand == cfg.OnDemand && c.cfg.RecordEnabled == cfg.RecordEnabled
}

func (c *channel) start() {
	c.cfgMu.RLock()
	record := c.cfg.RecordEnabled
	c.cfgMu.RUnlock()
	c.mu.Lock()
	if record && c.rec == nil {
		c.rec = newRecorderHLS(c)
		c.rec.start()
	}
	if c.run == nil {
		c.run = &puller{ch: c}
		c.run.start()
	}
	c.mu.Unlock()
}

func (c *channel) toggleRecorder(on bool) {
	c.mu.Lock()
	rec := c.rec
	if on {
		if rec == nil {
			rec = newRecorderHLS(c)
			c.rec = rec
			c.mu.Unlock()
			rec.start()
			return
		}
		c.mu.Unlock()
		return
	}
	if rec == nil {
		c.mu.Unlock()
		return
	}
	c.rec = nil
	c.mu.Unlock()
	rec.gracefulStop()
}

func (c *channel) effectiveSegmentSeconds() int {
	c.cfgMu.RLock()
	defer c.cfgMu.RUnlock()
	return c.cfg.SegmentSeconds
}

func (c *channel) updateSegment(seconds int) {
	c.cfgMu.Lock()
	c.cfg.SegmentSeconds = seconds
	c.cfgMu.Unlock()
}

func (c *channel) shutdown() {
	c.once.Do(func() {
		c.cancel()
		c.mu.Lock()
		run := c.run
		rec := c.rec
		c.mu.Unlock()
		if run != nil {
			run.stopWait()
		}
		if rec != nil {
			rec.gracefulStop()
		}
		c.mu.Lock()
		c.run = nil
		c.rec = nil
		c.mu.Unlock()
		c.hub.closeAll()
	})
}

func (c *channel) snapshot() (online bool, recording bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.online, c.rec != nil && c.rec.busy()
}

func (c *channel) setOnline(v bool) {
	c.mu.Lock()
	if c.ctx.Err() != nil {
		c.mu.Unlock()
		return
	}
	changed := c.online != v
	c.online = v
	c.mu.Unlock()
	if changed && c.ev != nil && c.ev.OnChannelStatus != nil {
		c.ev.OnChannelStatus(c.cfg.Id, v)
	}
}

// ------- trackInfo -------

func (c *channel) setTrackInfo(info TrackSpec) {
	c.mu.Lock()
	c.info = info
	ntf := c.infoNt
	c.mu.Unlock()
	select {
	case ntf <- struct{}{}:
	default:
	}
}

func (c *channel) resetTrackInfo() {
	c.mu.Lock()
	c.info = TrackSpec{}
	c.mu.Unlock()
}

func (c *channel) trackWait(ctx context.Context) (*TrackSpec, error) {
	if err := c.ctx.Err(); err != nil {
		return nil, ErrChannelNotFound
	}
	c.mu.RLock()
	snap := c.infoSnapshot()
	ntf := c.infoNt
	channelCtx := c.ctx
	c.mu.RUnlock()
	if snap.Video != nil || snap.Audio != nil {
		return snap, nil
	}
	select {
	case <-ntf:
	case <-channelCtx.Done():
		return nil, ErrChannelNotFound
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	c.mu.RLock()
	snap = c.infoSnapshot()
	c.mu.RUnlock()
	if snap.Video == nil && snap.Audio == nil {
		return nil, ErrChannelNoCodec
	}
	return snap, nil
}

func (c *channel) infoSnapshot() *TrackSpec {
	out := &TrackSpec{}
	if c.info.Video != nil {
		out.Video = &H264Spec{
			SPS: append([]byte(nil), c.info.Video.SPS...),
			PPS: append([]byte(nil), c.info.Video.PPS...),
		}
	}
	if c.info.Audio != nil {
		out.Audio = &AACSpec{SampleRate: c.info.Audio.SampleRate, ChannelCount: c.info.Audio.ChannelCount}
	}
	return out
}

func (c *channel) recPush(f *Frame) {
	c.mu.RLock()
	rec := c.rec
	c.mu.RUnlock()
	if rec != nil {
		rec.push(f)
	}
}
