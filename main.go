package main

import (
	"context"
	"fmt"
	record "github.com/libp2p/go-libp2p-record"
	"github.com/multiformats/go-multiaddr"
	"time"

	libp2p "github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	recordpb "github.com/libp2p/go-libp2p-record/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	"google.golang.org/protobuf/proto"
)

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
		"record": record.AcceptAll{},
	}
	kademliaDHT, err := dht.New(ctx, h, dht.Validator(validator))
	if err != nil {
		panic(err)
	}

	// Connect to Rust peer (update PORT and ID accordingly)
	rustAddrStr := "/ip4/127.0.0.1/tcp/56431/p2p/12D3KooWMuZpR6LwGvQu3rG1Cf9gtkJ1pb6B4mMrMe8frZVQChuu"
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

	// Wait for routing table to populate
	time.Sleep(3 * time.Second)
	fmt.Println("Wait for routing table to populate...")

	// Prepare Protobuf record
	rec := &recordpb.Record{
		Key:          []byte("/record/my-key-go"),
		Value:        []byte("hello from Go over protobuf"),
		TimeReceived: time.Now().Format(time.RFC3339),
	}
	data, err := proto.Marshal(rec)
	if err != nil {
		panic(err)
	}

	key := "/record/my-key-go"

	// PUT the serialized protobuf record
	fmt.Println("Putting protobuf record into DHT under key:", key)
	if err := kademliaDHT.PutValue(ctx, key, data); err != nil {
		panic("PutValue error: " + err.Error())
	}
	fmt.Println("Record put successfully!")

	// GET it back
	fmt.Println("Getting record from DHT...")
	val, err := kademliaDHT.GetValue(ctx, key)
	if err != nil {
		panic("GetValue error: " + err.Error())
	}
	fmt.Println("Raw data retrieved:", val)

	// Unmarshal Protobuf record
	got := &recordpb.Record{}
	if err := proto.Unmarshal(val, got); err != nil {
		panic("Failed to unmarshal record: " + err.Error())
	}

	fmt.Printf("Decoded Record:\n  key=%s\n  value=%s\n  timeReceived=%s\n",
		string(got.Key), string(got.Value), got.TimeReceived)
}
