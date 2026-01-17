package router

import "time"

type ServiceInstance struct {
	DeveloperAddress string    `json:"developerAddress"`
	URL              string    `json:"url"`
	Subgraph         string    `json:"subgraph"`
	Service          string    `json:"service"`
	LastSeen         time.Time `json:"lastSeen"`
	Type             string    `json:"type"`
	Path             string    `json:"path,omitempty"`
	ConfigCID        string    `json:"configCid,omitempty"`
}
