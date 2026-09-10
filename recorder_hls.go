package engine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/LanceAdd/gohlslib/v2"
	"github.com/LanceAdd/gohlslib/v2/pkg/codecs"
	"github.com/bluenviron/mediacommon/v2/pkg/codecs/mpeg4audio"
)

const (
	recorderQueueCap = 1500
	recorderLogEvery = 200
)

type recorderHLS struct {
	ch   *channel
	done chan struct{}

	mu       sync.Mutex
	cond     *sync.Cond
	queue    []*Frame
	stopping bool
	dropped  int64

	win *winWriter
}

func newRecorderHLS(ch *channel) *recorderHLS {
	r := &recorderHLS{ch: ch, done: make(chan struct{})}
	r.cond = sync.NewCond(&r.mu)
	return r
}

func (r *recorderHLS) start() {
	go r.loop()
}

func (r *recorderHLS) busy() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.win != nil
}

func (r *recorderHLS) push(f *Frame) {
	r.mu.Lock()
	if r.stopping {
		r.mu.Unlock()
		return
	}
	if len(r.queue) >= recorderQueueCap {
		copy(r.queue, r.queue[1:])
		r.queue[len(r.queue)-1] = f
		r.dropped++
		r.mu.Unlock()
		if r.dropped%recorderLogEvery == 0 {
			r.ch.env.Log.Warnf("[recorder:%s] queue full, dropped %d frames", r.ch.cfg.Id, r.dropped)
		}
		return
	}
	r.queue = append(r.queue, f)
	r.cond.Signal()
	r.mu.Unlock()
}

func (r *recorderHLS) gracefulStop() {
	r.mu.Lock()
	r.stopping = true
	r.cond.Signal()
	r.mu.Unlock()
	select {
	case <-r.done:
	case <-time.After(5 * time.Second):
		r.ch.env.Log.Warnf("[recorder:%s] gracefulStop timeout, abandon", r.ch.cfg.Id)
	}
}

func (r *recorderHLS) loop() {
	defer close(r.done)
	log := r.ch.env.Log
	var warnCount int
	for {
		r.mu.Lock()
		for len(r.queue) == 0 && !r.stopping {
			r.cond.Wait()
		}
		if len(r.queue) == 0 && r.stopping {
			r.mu.Unlock()
			break
		}
		f := r.queue[0]
		r.queue = r.queue[1:]
		r.mu.Unlock()

		if err := r.handle(f); err != nil {
			warnCount++
			if warnCount%50 == 1 {
				log.Warnf("[recorder:%s] write failed(%d): %v", r.ch.cfg.Id, warnCount, err)
			}
		}
	}
	r.closeWin()
}

func (r *recorderHLS) handle(f *Frame) error {
	r.mu.Lock()
	win := r.win
	r.mu.Unlock()

	if win != nil && f.SessionID != win.sid {
		r.closeWin()
		win = nil
	}
	if win != nil && f.Kind == KindVideo && f.PTS < win.endPTS {
		return nil
	}
	if win == nil {
		if f.Kind != KindVideo || !f.Key {
			return nil
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		info, err := r.ch.trackWait(ctx)
		cancel()
		if err != nil {
			return err
		}
		if err := r.openWin(info, f.SessionID); err != nil {
			return err
		}
		win = r.win
	} else if f.Kind == KindVideo && f.Key {
		if time.Since(win.start) >= time.Duration(r.ch.effectiveSegmentSeconds())*time.Second {
			r.closeWin()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			info, err := r.ch.trackWait(ctx)
			cancel()
			if err != nil {
				return nil
			}
			return r.openWin(info, f.SessionID)
		}
	}
	return win.write(f)
}

func WindowHLSDir(recordDir, rel string) string {
	return filepath.Join(recordDir, filepath.FromSlash(rel), "hls")
}

func WindowMP4Path(recordDir, rel string) string {
	dir := filepath.FromSlash(rel)
	return filepath.Join(recordDir, dir, "mp4", filepath.Base(dir)+".mp4")
}

func (r *recorderHLS) openWin(info *TrackSpec, sid uint64) error {
	if r.ch.effectiveSegmentSeconds() <= 0 {
		return fmt.Errorf("record segment seconds not set")
	}
	dir := time.Now().Format("20060102_150405")
	rel := filepath.ToSlash(filepath.Join(r.ch.cfg.Id, dir))
	absDir := WindowHLSDir(r.ch.env.RecordDir, rel)
	if err := os.MkdirAll(absDir, 0o755); err != nil {
		return err
	}
	muxer := &gohlslib.Muxer{
		Variant:            gohlslib.MuxerVariantMPEGTS,
		Archive:            true,
		SegmentMinDuration: 4 * time.Second,
		Directory:          absDir,
		OnEncodeError: func(err error) {
			r.ch.env.Log.Warnf("[recorder:%s] hls encode: %v", r.ch.cfg.Id, err)
		},
	}
	var vTrack, aTrack *gohlslib.Track
	if info.Video != nil && len(info.Video.SPS) > 0 && len(info.Video.PPS) > 0 {
		vTrack = &gohlslib.Track{Codec: &codecs.H264{SPS: info.Video.SPS, PPS: info.Video.PPS}, ClockRate: 90000}
		muxer.Tracks = append(muxer.Tracks, vTrack)
	}
	if info.Audio != nil {
		aTrack = &gohlslib.Track{
			Codec: &codecs.MPEG4Audio{Config: mpeg4audio.AudioSpecificConfig{
				Type: 2, SampleRate: info.Audio.SampleRate, ChannelConfig: uint8(info.Audio.ChannelCount),
			}},
			ClockRate: info.Audio.SampleRate,
		}
		muxer.Tracks = append(muxer.Tracks, aTrack)
	}
	if len(muxer.Tracks) == 0 {
		return fmt.Errorf("no track for window")
	}
	if err := muxer.Start(); err != nil {
		return err
	}
	r.mu.Lock()
	r.win = &winWriter{rec: r, mux: muxer, vTrack: vTrack, aTrack: aTrack, dir: absDir, rel: rel, start: time.Now(), sid: sid}
	r.mu.Unlock()
	r.ch.env.Log.Infof("[recorder:%s] window start %s", r.ch.cfg.Id, rel)
	return nil
}

func (r *recorderHLS) closeWin() {
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.closeWinInner()
	}()
	select {
	case <-done:
	case <-time.After(4 * time.Second):
		r.ch.env.Log.Warnf("[recorder:%s] closeWin timeout, abandoned window state", r.ch.cfg.Id)
	}
}

func (r *recorderHLS) closeWinInner() {
	r.mu.Lock()
	win := r.win
	r.win = nil
	r.mu.Unlock()
	if win == nil {
		return
	}
	win.mux.Close()
	list, _, size := scanSegments(win.dir)
	if len(list) == 0 {
		r.ch.env.Log.Warnf("[recorder:%s] close with no segment, removed %s", r.ch.cfg.Id, win.rel)
		_ = os.RemoveAll(win.dir)
		return
	}
	if r.ch.ev != nil && r.ch.ev.OnRecordClosed != nil {
		r.ch.ev.OnRecordClosed(RecordEvent{
			StreamId: r.ch.cfg.Id,
			FilePath: win.rel,
			Start:    win.start,
			End:      time.Now(),
			Size:     size,
			Segments: len(list),
		})
	}
	r.ch.env.Log.Infof("[recorder:%s] window end %s (%d segs, %d MB)", r.ch.cfg.Id, win.rel, len(list), size/1048576)
}

type winWriter struct {
	rec    *recorderHLS
	mux    *gohlslib.Muxer
	vTrack *gohlslib.Track
	aTrack *gohlslib.Track
	dir    string
	rel    string
	start  time.Time
	sid    uint64
	endPTS time.Duration
}

func (w *winWriter) write(f *Frame) error {
	if f.Kind == KindVideo && f.PTS > w.endPTS {
		w.endPTS = f.PTS
	}
	ntp := f.NTP
	if ntp.IsZero() {
		ntp = time.Now()
	}
	var err error
	if f.Kind == KindVideo {
		if w.vTrack == nil {
			return nil
		}
		err = w.mux.WriteH264(w.vTrack, ntp, ptsTick(f.PTS, w.vTrack.ClockRate), f.AU)
	} else {
		if w.aTrack == nil {
			return nil
		}
		err = w.mux.WriteMPEG4Audio(w.aTrack, ntp, ptsTick(f.PTS, w.aTrack.ClockRate), f.AU)
	}
	return err
}

func ptsTick(pts time.Duration, clockRate int) int64 {
	return int64(pts) * int64(clockRate) / int64(time.Second)
}

func scanSegments(dir string) ([]string, string, int64) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, "", 0
	}
	segRe := regexp.MustCompile(`^(.+)_seg(\d+)\.(mp4|ts)$`)
	type seg struct {
		name string
		id   uint64
	}
	var segs []seg
	var initName string
	var size int64
	for _, e := range entries {
		if m := segRe.FindStringSubmatch(e.Name()); m != nil {
			id, _ := strconv.ParseUint(m[2], 10, 64)
			segs = append(segs, seg{name: e.Name(), id: id})
		} else if strings.HasSuffix(e.Name(), "_init.mp4") {
			initName = e.Name()
		}
		if !e.IsDir() {
			if st, err := e.Info(); err == nil {
				size += st.Size()
			}
		}
	}
	sort.Slice(segs, func(i, j int) bool { return segs[i].id < segs[j].id })
	names := make([]string, 0, len(segs))
	for _, s := range segs {
		names = append(names, s.name)
	}
	return names, initName, size
}
