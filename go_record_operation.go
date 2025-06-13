package main

import (
	"context"
	"fmt"
	dht "github.com/libp2p/go-libp2p-kad-dht"
)

func StoreGoRecordLocally(ctx context.Context, kademliaDHT *dht.IpfsDHT) string {
	key := "/record/my-key-go"
	// Prepare go record
	rec := &GoRecord{
		Key:       []byte(key),
		Value:     []byte("hello from Go over protobuf"),
		Publisher: nil,
		Expires:   nil,
	}
	protobufRecord, err := rec.parseIntoProtobufRecord()
	if err != nil {
		panic("parseIntoProtobufRecord error: " + err.Error())
	}

	//store record locally
	if err := kademliaDHT.StoreRecord(ctx, key, protobufRecord); err != nil {
		panic("PutValue error: " + err.Error())
	}
	fmt.Println("Go Record stored locally successfully!")

	return key
}

func PutValueGoRecord(ctx context.Context, kademliaDHT *dht.IpfsDHT, peerID string) string {
	key := "/record/" + peerID
	// Prepare go record
	rec := &GoRecord{
		Key:       []byte(key),
		Value:     []byte("hello from Go over protobuf"),
		Publisher: nil,
		Expires:   nil,
	}

	val, err := rec.ToBytes()
	if err != nil {
		panic("Failed to serialize GoRecord: " + err.Error())
	}

	// Store it into the DHT using PutValue (key should be rec.Key)
	if err := kademliaDHT.PutValue(ctx, key, val); err != nil {
		panic("PutValue error: " + err.Error())
	}

	fmt.Println("PutValue GoRecord successfully!")
	return key
}
