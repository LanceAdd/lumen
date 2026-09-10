package engine

import "time"

type Kind uint8

const (
	KindVideo Kind = iota + 1
	KindAudio
)

type Frame struct {
	SessionID uint64
	Kind      Kind
	PTS       time.Duration
	NTP       time.Time
	Key       bool
	AU        [][]byte
}
