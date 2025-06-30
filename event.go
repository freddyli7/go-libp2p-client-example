package main

import (
	"github.com/libp2p/go-libp2p/core/peer"
	"time"
)

type EvtRecordPut struct {
	Key       string
	Target    []peer.ID
	Timestamp time.Time
}
