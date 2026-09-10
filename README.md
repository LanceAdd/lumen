# lumen

A Go media engine for RTSP cameras: pull streams, fan out frames to viewers, record continuously to HLS, remux recordings to MP4, and deliver live video over WebRTC.

## Features

- **RTSP pull** with exponential-backoff reconnect, a no-keyframe watchdog, TCP transport, and per-session epochs so downstream consumers can always tell which session a frame belongs to
- **Frame hub** fan-out: one bounded queue per viewer, slow viewers drop frames instead of blocking the pull loop
- **HLS recording** (MPEG-TS, archive mode): time-windowed directories with self-contained segments and final playlists, segment rotation at keyframes, and a session-aware recorder that starts a new window after every reconnect
- **MP4 remux**: lossless TS to MP4 conversion of a recorded window, with movie-time alignment that preserves the original audio/video start offset
- **WebRTC output** (`webrtcview` subpackage): H.264 track delivery driven by source PTS, with frame-rate adaptation, gap handling, and peer-connection lifecycle management
- **Unified timestamps**: audio and video share one synchronized clock (via gortsplib), with NTP fallback when RTCP sender reports are unavailable

## Requirements

- Go 1.26 or newer

## Install

```
go get github.com/LanceAdd/lumen
```

## Quick start

```go
package main

import (
    "context"
    "log"
    "time"

    "github.com/LanceAdd/lumen"
)

func main() {
    eng := engine.New(engine.Config{
        Context:        context.Background(),
        Log:            stdLogger{},
        RecordDir:      "/var/lib/lumen/record",
        ConnectTimeout: 3 * time.Second,
        ReadTimeout:    5 * time.Second,
    }, engine.Events{
        OnChannelStatus: func(streamID string, online bool) { /* update your online state */ },
        OnRecordClosed:  func(ev engine.RecordEvent) { /* index the finished window */ },
    })
    defer eng.Shutdown()

    err := eng.ApplyChannel(engine.ChannelConfig{
        Id:             "cam-01",
        Url:            "rtsp://user:pass@192.168.1.10/stream1",
        RecordEnabled:  true,
        SegmentSeconds: 300,
    })
    if err != nil {
        log.Fatal(err)
    }
}
```

`Config.Context` and `Config.Log` are required; `New` panics if they are missing.

## Watching frames

```go
sub, err := eng.Subscribe("cam-01")
if err != nil {
    return err
}
defer sub.Cancel()

for {
    select {
    case <-sub.Done:
        return
    case frame := <-sub.Frames:
        // frame.AU, frame.PTS, frame.NTP, frame.SessionID, frame.Key ...
    }
}
```

See the [Subscription type](viewer.go) documentation for lifecycle rules.

## WebRTC

```go
answer, err := webrtcview.Offer(ctx, webrtcview.OfferInput{
    Engine:        eng,
    Logger:        stdLogger{},
    StreamID:      streamID,
    SPS:           info.Video.SPS,
    OfferSDP:      offerSDP,
    ICEServerURLs: []string{"stun:stun.example.org:3478"},
})
```

## Recording layout and MP4

Each recording window is a directory:

```
{recordDir}/{streamId}/{20060102_150405}/hls/   segN.ts + index.m3u8 + playlists
```

Use the layout helpers and the remux helpers instead of hardcoding paths:

```go
hlsDir  := engine.WindowHLSDir(recordDir, rel)
mp4Path := engine.WindowMP4Path(recordDir, rel)

// One-shot remux into memory:
data, err := engine.RemuxWindowTS(hlsDir)

// Or remux and atomically write the MP4 (tmp file + rename):
size, err := engine.WriteWindowMP4(hlsDir, mp4Path)
```

## License

MIT
