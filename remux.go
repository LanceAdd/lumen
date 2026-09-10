package engine

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"

	"github.com/bluenviron/mediacommon/v2/pkg/codecs/h264"
	"github.com/bluenviron/mediacommon/v2/pkg/codecs/mpeg4audio"
	mp4codecs "github.com/bluenviron/mediacommon/v2/pkg/formats/mp4/codecs"
	"github.com/bluenviron/mediacommon/v2/pkg/formats/mpegts"
	mpegcodecs "github.com/bluenviron/mediacommon/v2/pkg/formats/mpegts/codecs"
	"github.com/bluenviron/mediacommon/v2/pkg/formats/pmp4"
)

const videoTimeScale = 90000

func RemuxWindowTS(hlsDir string) ([]byte, error) {
	segs, err := windowTsSegs(hlsDir)
	if err != nil {
		return nil, err
	}
	return tsToMp4(segs)
}

func WriteWindowMP4(hlsDir, mp4Path string) (int64, error) {
	data, err := RemuxWindowTS(hlsDir)
	if err != nil {
		return 0, err
	}
	if err := os.MkdirAll(filepath.Dir(mp4Path), 0o755); err != nil {
		return 0, err
	}
	tmp := mp4Path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return 0, err
	}
	if err := os.Rename(tmp, mp4Path); err != nil {
		return 0, err
	}
	return int64(len(data)), nil
}

func windowTsSegs(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	re := regexp.MustCompile(`^(.+?)_seg(\d+)\.ts$`)
	type seg struct {
		name string
		id   uint64
	}
	var segs []seg
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if m := re.FindStringSubmatch(e.Name()); m != nil {
			id, _ := strconv.ParseUint(m[2], 10, 64)
			segs = append(segs, seg{name: e.Name(), id: id})
		}
	}
	if len(segs) == 0 {
		return nil, fmt.Errorf("no ts segments in %s", dir)
	}
	sort.Slice(segs, func(i, j int) bool { return segs[i].id < segs[j].id })
	out := make([]string, 0, len(segs))
	for _, s := range segs {
		out = append(out, filepath.Join(dir, s.name))
	}
	return out, nil
}

func tsToMp4(segPaths []string) ([]byte, error) {
	var buf bytes.Buffer
	for _, p := range segPaths {
		data, err := os.ReadFile(p)
		if err != nil {
			return nil, err
		}
		buf.Write(data)
	}

	type vOut struct {
		dts   int64
		pts   int64
		avcc  []byte
		isKey bool
	}
	type aOut struct {
		pts  int64
		data []byte
	}
	var vS []vOut
	var aS []aOut
	var sps, pps []byte

	r := &mpegts.Reader{R: bufio.NewReader(bytes.NewReader(buf.Bytes()))}
	if err := r.Initialize(); err != nil {
		return nil, fmt.Errorf("mpegts reader: %w", err)
	}
	sampRate, chanCount := mpegtsTrackParams(r.Tracks())
	if sampRate <= 0 {
		return nil, fmt.Errorf("no aac audio track params")
	}
	for _, t := range r.Tracks() {
		switch t.Codec.(type) {
		case *mpegcodecs.H264:
			r.OnDataH264(t, func(pts int64, dts int64, au [][]byte) error {
				avcc, err := h264.AVCC(au).Marshal()
				if err != nil {
					return err
				}
				key := false
				for _, n := range au {
					if len(n) == 0 {
						continue
					}
					switch n[0] & 0x1f {
					case 7: // SPS
						if sps == nil {
							sps = n
						}
					case 8: // PPS
						if pps == nil {
							pps = n
						}
					case 5: // IDR
						key = true
					}
				}
				vS = append(vS, vOut{pts: pts, dts: dts, avcc: avcc, isKey: key})
				return nil
			})
		case *mpegcodecs.MPEG4Audio:
			r.OnDataMPEG4Audio(t, func(pts int64, aus [][]byte) error {
				for i, a := range aus {
					framePts := pts + int64(i)*mpeg4audio.SamplesPerAccessUnit*90000/int64(sampRate)
					aS = append(aS, aOut{pts: framePts, data: a})
				}
				return nil
			})
		}
	}
	for {
		if err := r.Read(); err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("mpegts read: %w", err)
		}
	}
	if len(vS) == 0 {
		return nil, fmt.Errorf("no video samples")
	}
	if sps == nil || pps == nil {
		return nil, fmt.Errorf("no h264 parameter set")
	}

	movie0 := vS[0].pts
	if len(aS) > 0 && aS[0].pts < movie0 {
		movie0 = aS[0].pts
	}
	videoTimeOffset := int32(vS[0].dts - movie0)
	var audioTimeOffset int32
	if len(aS) > 0 {
		audioTimeOffset = int32((aS[0].pts - movie0) * int64(sampRate) / 90000)
	}

	pres := &pmp4.Presentation{Tracks: []*pmp4.Track{
		{
			ID:        1,
			TimeScale: videoTimeScale,
			Codec:     &mp4codecs.H264{SPS: sps, PPS: pps},
		},
		{
			ID:        2,
			TimeScale: uint32(sampRate),
			Codec: &mp4codecs.MPEG4Audio{Config: mpeg4audio.AudioSpecificConfig{
				Type: 2, SampleRate: sampRate, ChannelConfig: uint8(chanCount),
			}},
		},
	}}
	pres.Tracks[0].TimeOffset = videoTimeOffset
	pres.Tracks[1].TimeOffset = audioTimeOffset

	for i := range vS {
		dur := uint32(3000)
		if i < len(vS)-1 {
			dur = uint32(vS[i+1].dts - vS[i].dts)
		} else if len(vS) > 1 {
			dur = uint32(vS[i].dts - vS[i-1].dts)
		}
		avcc := vS[i].avcc
		pres.Tracks[0].Samples = append(pres.Tracks[0].Samples, &pmp4.Sample{
			Duration:        dur,
			PTSOffset:       int32(vS[i].pts - vS[i].dts),
			IsNonSyncSample: !vS[i].isKey,
			PayloadSize:     uint32(len(avcc)),
			GetPayload:      func() ([]byte, error) { return avcc, nil },
		})
	}
	for i := range aS {
		data := aS[i].data
		pres.Tracks[1].Samples = append(pres.Tracks[1].Samples, &pmp4.Sample{
			Duration:    mpeg4audio.SamplesPerAccessUnit,
			PayloadSize: uint32(len(data)),
			GetPayload:  func() ([]byte, error) { return data, nil },
		})
	}

	var out bytes.Buffer
	if err := pres.Marshal(&out); err != nil {
		return nil, fmt.Errorf("mp4 marshal: %w", err)
	}
	return out.Bytes(), nil
}

func mpegtsTrackParams(tracks []*mpegts.Track) (int, int) {
	for _, t := range tracks {
		if aac, ok := t.Codec.(*mpegcodecs.MPEG4Audio); ok {
			return aac.Config.SampleRate, int(aac.Config.ChannelConfig)
		}
	}
	return 0, 0
}
