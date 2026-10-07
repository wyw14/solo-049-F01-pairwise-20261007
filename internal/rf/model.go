package rf

import "time"

type Polarization string

const (
	LeftCircular  Polarization = "LHCP"
	RightCircular Polarization = "RHCP"
	Linear        Polarization = "linear"
)

type State struct {
	ChainID          string       `json:"chain_id"`
	PassID           string       `json:"pass_id,omitempty"`
	CenterHz         int64        `json:"center_hz"`
	DopplerHz        int64        `json:"doppler_hz"`
	Polarization     Polarization `json:"polarization"`
	PlanUntil        time.Time    `json:"plan_until,omitempty"`
	TransmitEnabled  bool         `json:"transmit_enabled"`
	InhibitOperation string       `json:"inhibit_operation,omitempty"`
	UpdatedAt        time.Time    `json:"updated_at"`
}

type Plan struct {
	PassID       string       `json:"pass_id"`
	ChainID      string       `json:"chain_id"`
	CenterHz     int64        `json:"center_hz"`
	DopplerHz    int64        `json:"doppler_hz"`
	Polarization Polarization `json:"polarization"`
}
