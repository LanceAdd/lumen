package engine

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"github.com/bluenviron/gortsplib/v5"
	"github.com/bluenviron/gortsplib/v5/pkg/base"
	"github.com/bluenviron/gortsplib/v5/pkg/description"
	"github.com/bluenviron/gortsplib/v5/pkg/format"
	"github.com/pion/rtp"
)

var (
	errNoKeyframe        = errors.New("no keyframe")
	errUnsupportedStream = errors.New("unsupported stream (no h264/mpeg4audio track)")
)

type selectedVideoTrack struct {
	media  *description.Media
	format *format.H264
}

type selectedAudioTrack struct {
	media  *description.Media
	format *format.MPEG4Audio
}

type puller struct {
	ch   *channel
	done chan struct{}
	sid  atomic.Uint64
}

func (p *puller) start() {
	p.done = make(chan struct{})
	go p.run()
}

func (p *puller) stopWait() {
	<-p.done
}

func sleepSelect(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func (p *puller) run() {
	c := p.ch
	defer close(p.done)
	log := c.env.Log
	backoff := time.Second
	for {
		if c.ctx.Err() != nil {
			return
		}
		if err := p.pullOnce(c.ctx); err != nil {
			log.Warnf("[engine:%s] session end: %v, retry in %v", c.cfg.Id, err, backoff)
		}
		c.setOnline(false)
		c.resetTrackInfo()
		if c.ctx.Err() != nil {
			return
		}
		if !sleepSelect(c.ctx, backoff) {
			return
		}
		if backoff < 10*time.Second {
			backoff *= 2
		}
	}
}

func (p *puller) pullOnce(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c := p.ch
	c.cfgMu.RLock()
	url := c.cfg.Url
	c.cfgMu.RUnlock()
	log := c.env.Log

	sid := p.sid.Add(1)

	u, err := base.ParseURL(strings.TrimSpace(url))
	if err != nil {
		return err
	}
	protocol := gortsplib.ProtocolTCP
	dialer := &net.Dialer{Timeout: c.env.ConnectTimeout}
	client := &gortsplib.Client{
		Scheme:      u.Scheme,
		Host:        u.Host,
		Protocol:    &protocol,
		ReadTimeout: c.env.ReadTimeout,
		DialContext: func(dialCtx context.Context, network, address string) (net.Conn, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			combined, cancel := context.WithCancel(dialCtx)
			stop := context.AfterFunc(ctx, cancel)
			defer stop()
			defer cancel()
			return dialer.DialContext(combined, network, address)
		},
	}
	if err := client.Start(); err != nil {
		return err
	}
	stopWatch := context.AfterFunc(ctx, client.Close)
	defer stopWatch()
	defer client.Close()

	desc, _, err := client.Describe(u)
	if err != nil {
		return err
	}

	var video *selectedVideoTrack
	var audio *selectedAudioTrack
	for _, medi := range desc.Medias {
		if medi.IsBackChannel {
			continue
		}
		for _, forma := range medi.Formats {
			switch f := forma.(type) {
			case *format.H264:
				if video == nil {
					video = &selectedVideoTrack{media: medi, format: f}
				} else {
					log.Warnf("[engine:%s] ignore extra H264 track: media=%q control=%q", c.cfg.Id, medi.ID, medi.Control)
				}
			case *format.MPEG4Audio:
				if f.Config == nil {
					continue
				}
				if audio == nil {
					audio = &selectedAudioTrack{media: medi, format: f}
				} else {
					log.Warnf("[engine:%s] ignore extra AAC track: media=%q control=%q", c.cfg.Id, medi.ID, medi.Control)
				}
			}
		}
	}
	if video == nil && audio == nil {
		return errUnsupportedStream
	}

	selectedMedias := make(map[*description.Media]struct{})
	if video != nil {
		selectedMedias[video.media] = struct{}{}
	}
	if audio != nil {
		selectedMedias[audio.media] = struct{}{}
	}
	for medi := range selectedMedias {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, err := client.Setup(desc.BaseURL, medi, 0, 0); err != nil {
			return err
		}
	}

	info := TrackSpec{}
	var vdec *rtpVideoDecoder
	var adec *rtpAudioDecoder
	clock := &sessionClock{}
	if video != nil {
		dec, decErr := video.format.CreateDecoder()
		if decErr != nil {
			return decErr
		}
		vdec = &rtpVideoDecoder{d: dec, sid: sid}
		info.Video = &H264Spec{SPS: video.format.SPS, PPS: video.format.PPS}
	}
	if audio != nil {
		dec, decErr := audio.format.CreateDecoder()
		if decErr != nil {
			return decErr
		}
		adec = &rtpAudioDecoder{d: dec, sid: sid}
		info.Audio = &AACSpec{SampleRate: audio.format.Config.SampleRate, ChannelCount: int(audio.format.Config.ChannelConfig)}
	}

	var lastKey atomic.Int64
	lastKey.Store(time.Now().UnixNano())
	if video != nil {
		client.OnPacketRTP(video.media, video.format, func(pkt *rtp.Packet) {
			rawPTS, ok := client.PacketPTS(video.media, pkt)
			if !ok {
				return
			}
			pts := ptsDuration(rawPTS, video.format.ClockRate())
			packetNTP, ntpOK := client.PacketNTP(video.media, pkt)
			frame, derr := vdec.decode(pkt, pts, clock.ntp(pts, packetNTP, ntpOK))
			if derr != nil || frame == nil {
				return
			}
			if frame.Key {
				lastKey.Store(time.Now().UnixNano())
			}
			c.hub.fanout(frame)
			c.recPush(frame)
		})
	}
	if audio != nil {
		client.OnPacketRTP(audio.media, audio.format, func(pkt *rtp.Packet) {
			rawPTS, ok := client.PacketPTS(audio.media, pkt)
			if !ok {
				return
			}
			pts := ptsDuration(rawPTS, audio.format.ClockRate())
			packetNTP, ntpOK := client.PacketNTP(audio.media, pkt)
			frame, derr := adec.decode(pkt, pts, clock.ntp(pts, packetNTP, ntpOK))
			if derr != nil || frame == nil {
				return
			}
			c.recPush(frame)
		})
	}

	c.setTrackInfo(info)
	c.setOnline(true)
	log.Infof("[engine:%s] connected %s", c.cfg.Id, url)
	if _, err := client.Play(nil); err != nil {
		return err
	}

	watchdog := time.NewTicker(5 * time.Second)
	defer watchdog.Stop()
	waitCh := make(chan error, 1)
	go func() { waitCh <- client.Wait() }()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-waitCh:
			return err
		case <-watchdog.C:
			if time.Since(time.Unix(0, lastKey.Load())) > c.env.NoKeyframeAfter {
				log.Warnf("[engine:%s] no keyframe received, reconnect", c.cfg.Id)
				return errNoKeyframe
			}
		}
	}
}
