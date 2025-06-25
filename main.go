package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/ipfs/boxo/ipns"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-record/pb"
	"github.com/multiformats/go-multiaddr"
	"google.golang.org/protobuf/encoding/protowire"
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
	key := PutValueGoRecordWithPublisherExpires(ctx, kademliaDHT, peerInfo.ID.String())

	// store record by PutRecordAtPeer method(IpfsDHT.PutRecordAtPeer):: all works
	//key := PutRecordAtPeerGoRecord(ctx, kademliaDHT, peerInfo)
	//key := PutRecordAtPeerProtobufRecord(ctx, kademliaDHT, peerInfo)

	// get rust record by key and retrieved the raw bytes: works
	//key := "/record/my-key-rust"

	// GET it back
	fmt.Println("Getting record from DHT...")

	// Get value of record
	//val, err := kademliaDHT.GetValue(ctx, key)
	//if err != nil {
	//	panic("GetValue error: " + err.Error())
	//}

	// Get the entire record
	rec, err := kademliaDHT.GetRecord(ctx, key)
	if err != nil {
		panic("GetRecord error: " + err.Error())
	}
	fmt.Println("Raw data retrieved:", rec)

	// parse 666 to publisher and 777 to ttl
	pub, ttl, err := parsePublisherAndExpiresFromRustRecord(rec)
	if err != nil {
		panic(err)
	}

	exp := time.Now()
	if ttl != nil {
		exp = exp.Add(*ttl * time.Second)
	}

	fmt.Printf("Go record: %v\n", string(rec.Key))
	fmt.Printf("Go record: %v\n", string(rec.Value))
	fmt.Printf("Go record: %v\n", pub)
	fmt.Printf("Go record: %v\n", exp)

}

// parse 666 to publisher and 777 to ttl
func parsePublisherAndExpiresFromRustRecord(rec *pb.Record) (*peer.ID, *time.Duration, error) {
	msgReflect := rec.ProtoReflect()
	unknown := msgReflect.GetUnknown()

	var pub peer.ID
	var t uint64
	var err error

	for len(unknown) > 0 {
		num, typ, n := protowire.ConsumeTag(unknown)
		if n < 0 {
			return nil, nil, errors.New("failed to consume tag")
		}
		unknown = unknown[n:]

		switch num {
		case 666:
			if typ == protowire.BytesType {
				val, n := protowire.ConsumeBytes(unknown)
				if n < 0 {
					return nil, nil, errors.New("bad bytes for tag 666")
				}

				pub, err = peer.IDFromBytes(val)
				if err != nil {
					return nil, nil, errors.New("invalid peer ID bytes")
				}

				fmt.Printf("Publisher Peer ID: %s\n", pub)

				unknown = unknown[n:]
			}
		case 777:
			if typ == protowire.VarintType {
				t, n = protowire.ConsumeVarint(unknown)
				if n < 0 {
					return nil, nil, errors.New("bad variant for tag 777")
				}

				fmt.Printf("TTL: %d\n", t)

				unknown = unknown[n:]
			}
		default:
			fmt.Println("skip other unknown tag:", num)
		}
	}

	ttl := time.Duration(t)

	return &pub, &ttl, nil
}
