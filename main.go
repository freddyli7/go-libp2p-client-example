package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/ipfs/boxo/ipns"
	"github.com/libp2p/go-libp2p"
	"github.com/multiformats/go-multiaddr"
	"time"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	record "github.com/libp2p/go-libp2p-record"
	"github.com/libp2p/go-libp2p/core/peer"
)

// AcceptAllValidator is a permissive validator that accepts any value.
type AcceptAllValidator struct{}

// Validate always returns nil (accepts everything)
func (v AcceptAllValidator) Validate(key string, value []byte) error {
	return nil
}

// Select always returns 0 (no preference among records)
func (v AcceptAllValidator) Select(key string, vals [][]byte) (int, error) {
	if len(vals) == 0 {
		return -1, errors.New("no values to select from")
	}
	return 0, nil
}

func main() {
	ctx := context.Background()

	// Create a libp2p host
	h, err := libp2p.New()
	if err != nil {
		panic(err)
	}
	defer h.Close()
	fmt.Println("Go peer ID:", h.ID())

	// Initialize ChainSafe DHT with custom validator for "record" namespace
	validator := record.NamespacedValidator{
		"ipns": ipns.Validator{},
		"pk":   record.PublicKeyValidator{},
		// customized namespace
		"record": AcceptAllValidator{},
	}

	// Here we don't initialize the dual.DHT, instead we initialize a IpfsDHT
	// because dual.DHT is just a tuple with two IpfsDHT named LAN and WAN.
	// Our goal here is to test the newly added method on dual.DHT, specifically [dual.StoreRecord] and [dual.PutRecordTo]
	// the under methods they both are calling are [IpfsDHT.StoreRecord] and [IpfsDHT.PutRecordAtPeer]
	// so we only need to test [IpfsDHT.StoreRecord] and [IpfsDHT.PutRecordAtPeer] against the rust libp2p DHT host
	kademliaDHT, err := dht.New(ctx, h, dht.ProtocolPrefix("/record"), dht.Validator(validator))
	if err != nil {
		panic(err)
	}

	// Connect to Rust peer Alice
	rustAddrStr := "/ip4/127.0.0.1/tcp/8080/p2p/12D3KooWP2F2DdjvoPbgC8VLU1PH9WB1NTnjAXFNiWhbpioWYSbR"
	rustAddr, err := multiaddr.NewMultiaddr(rustAddrStr)
	if err != nil {
		panic(err)
	}
	peerInfo, err := peer.AddrInfoFromP2pAddr(rustAddr)
	if err != nil {
		panic(err)
	}
	if err := h.Connect(ctx, *peerInfo); err != nil {
		panic(err)
	}
	fmt.Println("Connected to Rust peer:", peerInfo.ID)

	ok, err := kademliaDHT.RoutingTable().TryAddPeer(peerInfo.ID, true, false)
	if err != nil {
		panic(err)
	}
	fmt.Println("Added peer:", ok)

	// Bootstrap the DHT to populate the routing table
	if _, err := kademliaDHT.FindPeer(ctx, peerInfo.ID); err != nil {
		panic("DHT find peer failed: " + err.Error())
	}
	fmt.Println("Peer found in routing table")

	// Wait for routing table to populate
	time.Sleep(3 * time.Second)
	fmt.Println("Wait for routing table to populate...")

	// store record locally(IpfsDHT.StoreRecord): all works
	//key := StoreGoRecordLocally(ctx, kademliaDHT)
	//key := StoreProtobufRecordLocally(ctx, kademliaDHT)

	// store record by put_value method(IpfsDHT.PutValue): all works
	//key := PutValueGoRecord(ctx, kademliaDHT, peerInfo.ID.String())
	//key := PutValueProtobufRecord(ctx, kademliaDHT, peerInfo.ID.String())

	// store record by PutRecordAtPeer method(IpfsDHT.PutRecordAtPeer):: all works
	//key := PutRecordAtPeerGoRecord(ctx, kademliaDHT, peerInfo)
	//key := PutRecordAtPeerProtobufRecord(ctx, kademliaDHT, peerInfo)

	// get rust record by key: works, retrieved the raw bytes
	key := "/record/my-key-rust"

	// GET it back
	fmt.Println("Getting record from DHT...")
	val, err := kademliaDHT.GetValue(ctx, key)
	if err != nil {
		panic("GetValue error: " + err.Error())
	}
	fmt.Println("Raw data retrieved:", val)

	//// decode the raw bytes of the value field
	//fmt.Println("Raw record value:", string(val))

	// Unmarshal Protobuf record
	//got := &recordpb.Record{}
	//if err := proto.Unmarshal(val, got); err != nil {
	//	panic("Failed to unmarshal record: " + err.Error())
	//}
	//
	//fmt.Printf("Decoded Record:\n  key=%s\n  value=%s\n  timeReceived=%s\n",
	//	string(got.Key), string(got.Value), got.TimeReceived)
}
