package webrtcview

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/bluenviron/mediacommon/v2/pkg/codecs/h264"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"

	"github.com/LanceAdd/lumen"
)

const noVideoTimeout = 15 * time.Second

var ErrInvalidOffer = errors.New("invalid offer")

type Logger interface {
	Warnf(format string, args ...interface{})
}

type OfferInput struct {
	Engine        *engine.Engine
	Logger        Logger
	StreamID      string
	SPS           []byte
	OfferSDP      string
	ICEServerURLs []string
}

func Offer(ctx context.Context, in OfferInput) (string, error) {
	track, err := webrtc.NewTrackLocalStaticSample(
		webrtc.RTPCodecCapability{
			MimeType:    webrtc.MimeTypeH264,
			SDPFmtpLine: h264Fmtp(in.SPS),
		},
		"video", "video",
	)
	if err != nil {
		return "", fmt.Errorf("create track: %w", err)
	}

	var ice []webrtc.ICEServer
	if len(in.ICEServerURLs) > 0 {
		ice = []webrtc.ICEServer{{URLs: in.ICEServerURLs}}
	}
	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{ICEServers: ice})
	if err != nil {
		return "", fmt.Errorf("create peerconnection: %w", err)
	}

	offer := webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: in.OfferSDP}
	if err := pc.SetRemoteDescription(offer); err != nil {
		pc.Close()
		return "", fmt.Errorf("%w: %v", ErrInvalidOffer, err)
	}
	if _, err := pc.AddTrack(track); err != nil {
		pc.Close()
		return "", fmt.Errorf("add track: %w", err)
	}
	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		pc.Close()
		return "", fmt.Errorf("create answer: %w", err)
	}
	if err := pc.SetLocalDescription(answer); err != nil {
		pc.Close()
		return "", fmt.Errorf("set local desc: %w", err)
	}
	waitGatheringComplete(ctx, pc, 2*time.Second)

	sub, err := in.Engine.Subscribe(in.StreamID)
	if err != nil {
		pc.Close()
		return "", err
	}
	go serve(in.Logger, sub, track, pc)
	return pc.LocalDescription().SDP, nil
}

func serve(log Logger, sub *engine.Subscription, track *webrtc.TrackLocalStaticSample, pc *webrtc.PeerConnection) {
	defer sub.Cancel()
	defer pc.Close()
	sessCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pc.OnConnectionStateChange(func(st webrtc.PeerConnectionState) {
		switch st {
		case webrtc.PeerConnectionStateFailed,
			webrtc.PeerConnectionStateDisconnected,
			webrtc.PeerConnectionStateClosed:
			cancel()
		}
	})
	var videoBuffer videoSampleBuffer
	noVideo := time.NewTimer(noVideoTimeout)
	defer noVideo.Stop()
	for {
		select {
		case <-sessCtx.Done():
			return
		case <-sub.Done:
			return
		case <-noVideo.C:
			log.Warnf("[webrtcview] viewer exit: no video in %v", noVideoTimeout)
			return
		case frame, ok := <-sub.Frames:
			if !ok {
				return
			}
			if frame.Kind != engine.KindVideo {
				continue
			}
			if frame.Key {
				noVideo.Reset(noVideoTimeout)
			}
			if err := videoBuffer.push(frame, func(frame *engine.Frame, duration time.Duration) error {
				data, err := h264.AnnexB(frame.AU).Marshal()
				if err != nil {
					return err
				}
				return track.WriteSample(media.Sample{
					Data:      data,
					Duration:  duration,
					Timestamp: time.Now(),
				})
			}); err != nil {
				log.Warnf("[webrtcview] viewer exit: %v", err)
				return
			}
		}
	}
}

func h264Fmtp(sps []byte) string {
	if len(sps) >= 4 {
		return fmt.Sprintf("packetization-mode=1;profile-level-id=%02x%02x%02x", sps[1], sps[2], sps[3])
	}
	return "packetization-mode=1"
}

func waitGatheringComplete(ctx context.Context, pc *webrtc.PeerConnection, timeout time.Duration) {
	done := make(chan struct{})
	pc.OnICEGatheringStateChange(func(state webrtc.ICEGatheringState) {
		if state == webrtc.ICEGatheringStateComplete {
			close(done)
		}
	})
	select {
	case <-done:
	case <-time.After(timeout):
	case <-ctx.Done():
	}
}
