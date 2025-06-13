package main

import (
	"context"
	"fmt"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	recordpb "github.com/libp2p/go-libp2p-record/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	"google.golang.org/protobuf/proto"
	"time"
)

func StoreProtobufRecordLocally(ctx context.Context, kademliaDHT *dht.IpfsDHT) string {
	key := "/record/my-key-go"

	// Prepare Protobuf record
	rec := &recordpb.Record{
		Key:          []byte("/record/my-key-go"),
		Value:        []byte("hello from Go over protobuf"),
		TimeReceived: time.Now().Format(time.RFC3339),
	}

	// store record locally
	if err := kademliaDHT.StoreRecord(ctx, key, rec); err != nil {
		panic("PutValue error: " + err.Error())
	}
	fmt.Println("Protobuf Record stored locally successfully!")

	return key
}

func PutValueProtobufRecord(ctx context.Context, kademliaDHT *dht.IpfsDHT, peerID string) string {
	key := "/record/" + peerID
	// Prepare Protobuf record
	rec := &recordpb.Record{
		Key:          []byte(key),
		Value:        []byte("hello from Go over protobuf"),
		TimeReceived: time.Now().Format(time.RFC3339),
	}

	data, err := proto.Marshal(rec)
	if err != nil {
		panic(err)
	}

	// Store it into the DHT using PutValue (key should be rec.Key)
	if err := kademliaDHT.PutValue(ctx, key, data); err != nil {
		panic("PutValue error: " + err.Error())
	}

	fmt.Println("PutValue Protobuf Record successfully!")
	return key
}

func PutRecordAtPeerProtobufRecord(ctx context.Context, kademliaDHT *dht.IpfsDHT, peerInfo *peer.AddrInfo) string {
	key := "/record/" + peerInfo.ID
	// Prepare Protobuf record
	rec := &recordpb.Record{
		Key:          []byte(key),
		Value:        []byte("hello from Go over protobuf"),
		TimeReceived: time.Now().Format(time.RFC3339),
	}

	// Store it into the DHT using PutRecordAt
	peers := []peer.AddrInfo{
		*peerInfo,
	}
	if err := kademliaDHT.PutRecordAtPeer(ctx, rec, peers); err != nil {
		panic("PutValue error: " + err.Error())
	}

	fmt.Println("PutRecordAt Protobuf Record successfully!")
	return ""
}
