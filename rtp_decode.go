package engine

import (
	"sync"
	"time"

	"github.com/bluenviron/gortsplib/v5/pkg/format/rtph264"
	"github.com/bluenviron/gortsplib/v5/pkg/format/rtpmpeg4audio"
	"github.com/bluenviron/mediacommon/v2/pkg/codecs/h264"
	"github.com/pion/rtp"
)

type rtpVideoDecoder struct {
	d   *rtph264.Decoder
	sid uint64
}

func (v *rtpVideoDecoder) decode(pkt *rtp.Packet, pts time.Duration, ntp time.Time) (*Frame, error) {
	au, err := v.d.Decode(pkt)
	if err != nil {
		return nil, err
	}
	if len(au) == 0 {
		return nil, nil
	}

	return &Frame{SessionID: v.sid, Kind: KindVideo, PTS: pts, NTP: ntp, Key: h264.IsRandomAccess(au), AU: au}, nil
}

type rtpAudioDecoder struct {
	d   *rtpmpeg4audio.Decoder
	sid uint64
}

func (a *rtpAudioDecoder) decode(pkt *rtp.Packet, pts time.Duration, ntp time.Time) (*Frame, error) {
	aus, err := a.d.Decode(pkt)
	if err != nil {
		return nil, err
	}
	if len(aus) == 0 {
		return nil, nil
	}
	return &Frame{SessionID: a.sid, Kind: KindAudio, PTS: pts, NTP: ntp, AU: aus}, nil
}

func ptsDuration(pts int64, clockRate int) time.Duration {
	if clockRate <= 0 {
		return 0
	}
	rate := int64(clockRate)
	secs := pts / rate
	rem := pts % rate
	return time.Duration(secs)*time.Second + time.Duration(rem)*time.Second/time.Duration(rate)
}

type sessionClock struct {
	mu      sync.Mutex
	baseNTP time.Time
}

func (c *sessionClock) ntp(pts time.Duration, packetNTP time.Time, available bool) time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	if available {
		if c.baseNTP.IsZero() {
			c.baseNTP = packetNTP.Add(-pts)
		}
		return packetNTP
	}
	if c.baseNTP.IsZero() {
		c.baseNTP = time.Now().Add(-pts)
	}
	return c.baseNTP.Add(pts)
}
