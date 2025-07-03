package main

import (
	"context"
	"fmt"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p-record/pb"
	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/peer"
	"google.golang.org/protobuf/proto"
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

// PutRecordAtPeerGoRecordWithPublisherExpiresManuallyConstruct manually constructs the Record with publisher and expires fields without modifying the Record.proto
func PutRecordAtPeerGoRecordWithPublisherExpiresManuallyConstruct(ctx context.Context, kademliaDHT *dht.IpfsDHT, eventBus event.Bus, peerInfo PeerInfo, peerAddr peer.AddrInfo) string {
	key := "/record/" + peerInfo.ConnectingPeerID.String()
	t := time.Now().Add(300 * time.Second)
	p := peerInfo.Publisher

	// Prepare go record
	rec := &GoRecord{
		Key:       []byte(key),
		Value:     []byte("hello from Go over protobuf"),
		Publisher: &p,
		Expires:   &t,
	}

	recordData, err := rec.appendPublisherAndExpiresToGoProtobufRecord()
	if err != nil {
		panic("Failed to serialize GoRecord: " + err.Error())
	}

	// verify pb.Record by unmarshalling it
	a := &pb.Record{}
	err = proto.Unmarshal(recordData, a)
	if err != nil {
		panic("fucked here")
	}
	fmt.Printf("Unmarshal protobuf: %+v\n", a)

	// Note: publisher can not be the connecting peer
	if err = kademliaDHT.PutRecordAtPeer(ctx, a, []peer.AddrInfo{peerAddr}); err != nil {
		panic("PutRecordAtPeer error: " + err.Error())
	}

	// emit the event
	emitter, err := eventBus.Emitter(new(EvtRecordPut))
	if err != nil {
		panic("Emitter creation error: " + err.Error())
	}
	err = emitter.Emit(EvtRecordPut{
		Key:       key,
		Target:    []peer.ID{peerAddr.ID},
		Timestamp: time.Now(),
	})
	if err != nil {
		panic("Emit event error: " + err.Error())
	}
	defer emitter.Close()

	fmt.Println("PutRecordAtPeer GoRecord successfully!")
	return key
}

// PutRecordAtPeerGoRecordWithPublisherExpiresUpdateProtoMessage constructs the Record with publisher and expires fields after modifying the Record.proto
func PutRecordAtPeerGoRecordWithPublisherExpiresUpdateProtoMessage(ctx context.Context, kademliaDHT *dht.IpfsDHT, eventBus event.Bus, peerInfo PeerInfo, peerAddr peer.AddrInfo) string {
	key := "/record/" + peerInfo.ConnectingPeerID.String()
	t := time.Now().Add(300 * time.Second)
	p := peerInfo.Publisher

	// Prepare go record
	rec := &GoRecord{
		Key:       []byte(key),
		Value:     []byte("hello from Go over protobuf"),
		Publisher: &p,
		Expires:   &t,
	}

	// prepare updated protobuf record
	a := &pb.Record{
		Key:       rec.Key,
		Value:     rec.Value,
		Publisher: peerInfo.PublisherBytes,
		Ttl:       300,
	}

	// Note: publisher can not be the connecting peer
	if err := kademliaDHT.PutRecordAtPeer(ctx, a, []peer.AddrInfo{peerAddr}); err != nil {
		panic("PutRecordAtPeer error: " + err.Error())
	}

	//// emit the event
	//emitter, err := eventBus.Emitter(new(EvtRecordPut))
	//if err != nil {
	//	panic("Emitter creation error: " + err.Error())
	//}
	//err = emitter.Emit(EvtRecordPut{
	//	Key:       key,
	//	Target:    []peer.ID{peerAddr.ID},
	//	Timestamp: time.Now(),
	//})
	//if err != nil {
	//	panic("Emit event error: " + err.Error())
	//}
	//defer emitter.Close()

	fmt.Println("PutRecordAtPeer GoRecord successfully!")
	return key
}
