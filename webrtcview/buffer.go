package webrtcview

import (
	"time"

	"github.com/LanceAdd/lumen"
)

const (
	minVideoFrameDuration = time.Millisecond
	maxContinuousVideoGap = 2 * time.Second
)

// videoSampleBuffer holds one frame so its Duration can be derived from the next PTS.
// State is local to one viewer because Hub subscribers can drop different frames.
type videoSampleBuffer struct {
	pending *engine.Frame
}

func (b *videoSampleBuffer) reset(frame *engine.Frame) {
	b.pending = nil
	if frame != nil && frame.Key {
		b.pending = frame
	}
}

func (b *videoSampleBuffer) push(
	frame *engine.Frame,
	write func(*engine.Frame, time.Duration) error,
) error {
	if b.pending == nil {
		b.reset(frame)
		return nil
	}
	if frame.SessionID != b.pending.SessionID {
		b.reset(frame)
		return nil
	}

	duration := frame.PTS - b.pending.PTS
	if duration <= 0 {
		if frame.Key {
			b.reset(frame)
		}
		return nil
	}
	if duration > maxContinuousVideoGap {
		// A long gap is a discontinuity, not a sample duration. Rebase at a keyframe.
		b.reset(frame)
		return nil
	}
	if duration < minVideoFrameDuration {
		duration = minVideoFrameDuration
	}
	if err := write(b.pending, duration); err != nil {
		return err
	}
	b.pending = frame
	return nil
}
