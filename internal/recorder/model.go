package recorder

import "time"

type SegmentStatus string

const (
	Open     SegmentStatus = "open"
	Draining SegmentStatus = "draining"
	Sealed   SegmentStatus = "sealed"
	Aborted  SegmentStatus = "aborted"
)

type Frame struct {
	Sequence uint64    `json:"sequence"`
	Epoch    int64     `json:"epoch"`
	Received time.Time `json:"received"`
	Payload  []byte    `json:"payload"`
	CRC32    uint32    `json:"crc32"`
}

type Segment struct {
	ID              string        `json:"id"`
	PassID          string        `json:"pass_id"`
	OwnerStation    string        `json:"owner_station"`
	OwnerGeneration int64         `json:"owner_generation"`
	Status          SegmentStatus `json:"status"`
	FramesWritten   int           `json:"frames_written"`
	FramesBuffered  int           `json:"frames_buffered"`
	StartedAt       time.Time     `json:"started_at"`
	ExpectedLOS     time.Time     `json:"expected_los,omitempty"`
	SealedAt        time.Time     `json:"sealed_at,omitempty"`
	SealReason      string        `json:"seal_reason,omitempty"`
}

type SealResult struct {
	Segment  Segment `json:"segment"`
	Complete bool    `json:"complete"`
	Error    string  `json:"error,omitempty"`
}
