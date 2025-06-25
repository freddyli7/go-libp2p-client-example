package main

import (
	"bytes"
	"encoding/binary"
	record "github.com/libp2p/go-libp2p-record"
	recordpb "github.com/libp2p/go-libp2p-record/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	"google.golang.org/protobuf/encoding/protowire"
	"time"
)

// GoRecord matches the fields of rust version of Record used in polkadot-sdk
type GoRecord struct {
	Key       []byte
	Value     []byte
	Publisher *peer.ID
	Expires   *time.Time
}

func (r *GoRecord) ToBytes() ([]byte, error) {
	buf := new(bytes.Buffer)

	// Helper: write a length-prefixed byte slice
	writeBytes := func(data []byte) error {
		if err := binary.Write(buf, binary.BigEndian, uint32(len(data))); err != nil {
			return err
		}
		_, err := buf.Write(data)
		return err
	}

	// Serialize Key
	if err := writeBytes(r.Key); err != nil {
		return nil, err
	}

	// Serialize Value
	if err := writeBytes(r.Value); err != nil {
		return nil, err
	}

	// Serialize Publisher ID
	if r.Publisher != nil {
		pubBytes := []byte(r.Publisher.String())
		if err := writeBytes(pubBytes); err != nil {
			return nil, err
		}
	} else {
		if err := writeBytes([]byte{}); err != nil {
			return nil, err
		}
	}

	// Serialize Expires time
	if r.Expires != nil {
		ts := r.Expires.UnixNano()
		if err := binary.Write(buf, binary.BigEndian, ts); err != nil {
			return nil, err
		}
	} else {
		// Write 0 if nil
		if err := binary.Write(buf, binary.BigEndian, int64(0)); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

func (r *GoRecord) parseIntoProtobufRecord() (*recordpb.Record, error) {
	value, err := r.ToBytes()
	if err != nil {
		return nil, err
	}
	return record.MakePutRecord(string(r.Key), value), nil
}

// appendPublisherAndExpiresToGoProtobufRecord manually constructs the go protobuf record type by adding unknownFields
// publisher(666) and expires(777)
func (r *GoRecord) appendPublisherAndExpiresToGoProtobufRecord() ([]byte, error) {
	publisherBytes := *r.Publisher

	var buf []byte

	// Field 1: key
	buf = append(buf, protowire.AppendTag(nil, 1, protowire.BytesType)...)
	buf = append(buf, protowire.AppendBytes(nil, r.Key)...)

	// Field 2: value
	buf = append(buf, protowire.AppendTag(nil, 2, protowire.BytesType)...)
	buf = append(buf, protowire.AppendBytes(nil, r.Value)...)

	// Field 666: publisher
	buf = append(buf, protowire.AppendTag(nil, 666, protowire.BytesType)...)
	buf = append(buf, protowire.AppendBytes(nil, []byte(publisherBytes))...)

	// Field 777: expires (TTL)
	buf = append(buf, protowire.AppendTag(nil, 777, protowire.VarintType)...)
	buf = append(buf, protowire.AppendVarint(nil, uint64(r.Expires.UnixNano()))...)

	return buf, nil
}
