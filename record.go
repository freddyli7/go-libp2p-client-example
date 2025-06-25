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

func (r *GoRecord) appendPublisherAndExpiresToGetProtobufRecord() ([]byte, error) {
	return buildRustCompatibleRecord(r.Key, r.Value, []byte(r.Publisher.String()), uint64(r.Expires.Second()))
}

func buildRustCompatibleRecord(
	key []byte,
	value []byte,
	publisher []byte,
	ttl uint64,
) ([]byte, error) {
	var buf []byte

	// Field 1: key (bytes)
	buf = append(buf, protowire.AppendTag(nil, 1, protowire.BytesType)...)
	buf = append(buf, protowire.AppendBytes(nil, key)...)

	// Field 2: value (bytes)
	buf = append(buf, protowire.AppendTag(nil, 2, protowire.BytesType)...)
	buf = append(buf, protowire.AppendBytes(nil, value)...)

	// Field 5: timeReceived (hardcoded)
	buf = append(buf, protowire.AppendTag(nil, 5, protowire.BytesType)...)
	buf = append(buf, protowire.AppendString(nil, time.Now().Format(time.RFC3339))...)

	// Field 666: publisher (bytes)
	buf = append(buf, protowire.AppendTag(nil, 666, protowire.BytesType)...)
	buf = append(buf, protowire.AppendBytes(nil, publisher)...)

	// Field 777: ttl (uint32)
	buf = append(buf, protowire.AppendTag(nil, 777, protowire.VarintType)...)
	buf = append(buf, protowire.AppendVarint(nil, ttl)...)

	return buf, nil
}

// not working
//func buildRustCompatibleRecord(key []byte, value []byte, publisher []byte, ttl uint64) ([]byte, error) {
//	fmt.Println(key)
//	fmt.Println(value)
//	fmt.Println(publisher)
//	fmt.Println(ttl)
//
//	rec := &recordpb.Record{
//		Key:          key,
//		Value:        value,
//		TimeReceived: time.Now().Format(time.RFC3339),
//	}
//
//	// Manually encode publisher (field 666) and ttl (field 777)
//	var extra []byte
//
//	// Field 666: bytes publisher
//	extra = append(extra, protowire.AppendTag(nil, 666, protowire.BytesType)...)
//	extra = append(extra, protowire.AppendBytes(nil, publisher)...)
//
//	// Field 777: uint32 ttl
//	extra = append(extra, protowire.AppendTag(nil, 777, protowire.VarintType)...)
//	extra = append(extra, protowire.AppendVarint(nil, ttl)...)
//
//	// Inject into unknown fields
//	rec.ProtoReflect().SetUnknown(extra)
//
//	// Marshal the full message
//	return proto.Marshal(rec)
//}
