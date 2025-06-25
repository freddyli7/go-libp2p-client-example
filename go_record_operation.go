package main

import (
	"context"
	"fmt"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/peer"
	"time"
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

func PutRecordAtPeerGoRecord(ctx context.Context, kademliaDHT *dht.IpfsDHT, peerInfo *peer.AddrInfo) string {
	key := "/record/" + peerInfo.ID.String()
	fmt.Println(key)
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

	// Store it into the DHT using PutRecordAt
	peers := []peer.AddrInfo{
		*peerInfo,
	}
	if err := kademliaDHT.PutRecordAtPeer(ctx, protobufRecord, peers); err != nil {
		panic("PutRecordAt error: " + err.Error())
	}

	fmt.Println("PutRecordAt GoRecord successfully!")

	return key
}

func PutValueGoRecordWithPublisherExpires(ctx context.Context, kademliaDHT *dht.IpfsDHT, peerID string) string {
	key := "/record/" + peerID
	p := peer.ID(peerID)
	t := time.Now().Add(300 * time.Second)

	// Prepare go record
	rec := &GoRecord{
		Key:       []byte(key),
		Value:     []byte("hello from Go over protobuf"),
		Publisher: &p,
		Expires:   &t,
	}

	recordData, err := rec.appendPublisherAndExpiresToGetProtobufRecord()
	if err != nil {
		panic("Failed to serialize GoRecord: " + err.Error())
	}

	fmt.Println(recordData)

	// Store it into the DHT using PutValue (key should be rec.Key)
	if err := kademliaDHT.PutValue(ctx, key, recordData); err != nil {
		panic("PutValue error: " + err.Error())
	}

	fmt.Println("PutValue GoRecord successfully!")
	return key
}
