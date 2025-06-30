package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/ipfs/boxo/ipns"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-record/pb"
	"github.com/libp2p/go-libp2p/core/event"
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

type PeerInfo struct {
	ConnectingEndpoint string
	ConnectingPeerID   peer.ID
	Publisher          peer.ID
	PublisherBytes     []byte
}

func main() {
	ctx := context.Background()

	// prepare peer info
	bobPeerIDBytes := []byte{0, 36, 8, 1, 18, 32, 144, 15, 46, 116, 214, 3, 224, 245, 171, 197, 194, 89, 140, 4, 67, 135, 70, 45, 123, 38, 175, 168, 83, 242, 57, 93, 216, 66, 15, 78, 104, 67}
	alicePeerIDBytes := []byte{0, 36, 8, 1, 18, 32, 196, 50, 209, 144, 216, 163, 34, 240, 217, 233, 20, 206, 138, 167, 109, 93, 166, 219, 179, 77, 144, 221, 99, 41, 241, 53, 78, 243, 160, 76, 196, 120}
	alicePeerID, err := peer.IDFromBytes(alicePeerIDBytes)
	if err != nil {
		panic(err)
	}
	bobPeerID, err := peer.IDFromBytes(bobPeerIDBytes)
	if err != nil {
		panic(err)
	}
	connectingPeers := map[string]PeerInfo{
		"connecting_to_alice": {
			ConnectingEndpoint: "/ip4/127.0.0.1/tcp/8080/p2p/12D3KooWP2F2DdjvoPbgC8VLU1PH9WB1NTnjAXFNiWhbpioWYSbR",
			ConnectingPeerID:   alicePeerID,
			Publisher:          bobPeerID,
			PublisherBytes:     bobPeerIDBytes,
		},
		"connecting_to_bob": {
			ConnectingEndpoint: "/ip4/127.0.0.1/tcp/8081/p2p/12D3KooWKWiJaRrKxxq6PwxdWFbg2uou5ejM6NGAgzotgsDWvvn6",
			ConnectingPeerID:   bobPeerID,
			Publisher:          alicePeerID,
			PublisherBytes:     alicePeerIDBytes,
		},
	}

	// choose the connecting peer
	// Note: change the query key in the rust example code
	whoToConnect := "connecting_to_alice"

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

	eventBus := h.EventBus()

	// Subscribe events using go-libp2p repo event
	peerConnSub, err := eventBus.Subscribe(new(event.EvtPeerConnectednessChanged))
	if err != nil {
		panic(err)
	}
	defer peerConnSub.Close()
	recordStoredSub, err := eventBus.Subscribe(new(EvtRecordPut))
	if err != nil {
		panic(err)
	}
	defer recordStoredSub.Close()

	// print subscribed event info
	go func() {
		for e := range peerConnSub.Out() {
			evt := e.(event.EvtPeerConnectednessChanged)
			fmt.Printf("[Peer Event] %s is now %s\n", evt.Peer, evt.Connectedness)
		}
	}()
	go func() {
		for e := range recordStoredSub.Out() {
			evt := e.(EvtRecordPut)
			fmt.Printf("[Record Event] %s Record stored %s at %s\n", evt.Timestamp, evt.Key, evt.Target)
		}
	}()

	// Here we don't initialize the dual.DHT, instead we initialize a IpfsDHT
	// because dual.DHT is just a tuple with two IpfsDHT named LAN and WAN.
	// Our goal here is to test the newly added method on dual.DHT, specifically [dual.StoreRecord] and [dual.PutRecordTo]
	// the under methods they both are calling are [IpfsDHT.StoreRecord] and [IpfsDHT.PutRecordAtPeer]
	// so we only need to test [IpfsDHT.StoreRecord] and [IpfsDHT.PutRecordAtPeer] against the rust libp2p DHT host
	kademliaDHT, err := dht.New(ctx, h, dht.ProtocolPrefix("/record"), dht.Validator(validator))
	if err != nil {
		panic(err)
	}

	// Connect to Rust peer
	rustAddr, err := multiaddr.NewMultiaddr(connectingPeers[whoToConnect].ConnectingEndpoint)
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

	// ---------------------- Test cases ------------------ //
	// store record locally(IpfsDHT.StoreRecord): all works
	//key := StoreGoRecordLocally(ctx, kademliaDHT)
	//key := StoreProtobufRecordLocally(ctx, kademliaDHT)

	// store record by put_value method(IpfsDHT.PutValue): all works
	//key := PutValueGoRecord(ctx, kademliaDHT, peerInfo.ID.String())
	//key := PutValueProtobufRecord(ctx, kademliaDHT, peerInfo.ID.String())

	// store record by PutRecordAtPeer method(IpfsDHT.PutRecordAtPeer):: all works
	//key := PutRecordAtPeerGoRecord(ctx, kademliaDHT, peerInfo)
	//key := PutRecordAtPeerProtobufRecord(ctx, kademliaDHT, peerInfo)

	// adding publisher and expires fields that can be properly parsed from rust side
	// manually add extra fields without modifying the protobuf message
	//key := PutRecordAtPeerGoRecordWithPublisherExpiresManuallyConstruct(ctx, kademliaDHT, eventBus, connectingPeers[whoToConnect], *peerInfo)
	// updating the protobuf message with desired fields
	key := PutRecordAtPeerGoRecordWithPublisherExpiresUpdateProtoMessage(ctx, kademliaDHT, eventBus, connectingPeers[whoToConnect], *peerInfo)

	// get rust record by key and retrieved the raw bytes: works
	//key := "/record/my-key-rust"

	// GET it back
	fmt.Println("Getting record from DHT...")

	// Get value of record
	//val, err := kademliaDHT.GetValue(ctx, key)
	//if err != nil {
	//	panic("GetValue error: " + err.Error())
	//}
	//fmt.Println("Raw value retrieved:", val)

	// Get the entire record and parse publisher and expires fields
	rec, err := kademliaDHT.GetRecord(ctx, key)
	if err != nil {
		panic("GetRecord error: " + err.Error())
	}
	fmt.Println("Raw data retrieved:", rec)

	// manually parse 666 to publisher and 777 to ttl
	//pub, ttl, err := parsePublisherAndExpiresFromRustRecord(rec)
	//if err != nil {
	//	panic(err)
	//}
	//
	//exp := time.Now()
	//if ttl != nil {
	//	exp = exp.Add(*ttl * time.Second)
	//}

	// update Record protobuf message
	// publisher and expires are available fields in the Record type
	pub, err := peer.IDFromBytes(rec.Publisher)
	if err != nil {
		panic(err)
	}
	exp := rec.Ttl

	fmt.Printf("Go record key: %v\n", string(rec.Key))
	fmt.Printf("Go record value: %v\n", string(rec.Value))
	fmt.Printf("Go record publisher: %v\n", pub)
	fmt.Printf("Go record expires: %v\n", exp)
}

// manually parse 666 to publisher and 777 to ttl when publisher and expires are in the unknown fields of Record type
// like this :
//
//	type Record struct {
//		state protoimpl.MessageState `protogen:"open.v1"`
//		// The key that references this record
//		Key []byte `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
//		// The actual value this record is storing
//		Value []byte `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
//		// Time the record was received, set by receiver
//		TimeReceived  string `protobuf:"bytes,5,opt,name=timeReceived,proto3" json:"timeReceived,omitempty"`
//		unknownFields protoimpl.UnknownFields
//		sizeCache     protoimpl.SizeCache
//	}
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
