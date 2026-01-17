package p2p

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	gwcrypto "grapthway/pkg/crypto"
	"grapthway/pkg/ledger/types"
	"grapthway/pkg/model"
	"grapthway/pkg/monitoring"
	pb "grapthway/pkg/proto"
	modeltoproto "grapthway/pkg/proto/converter/model-to-proto"
	prototomodel "grapthway/pkg/proto/converter/proto-to-model"
	"grapthway/pkg/util"

	json "github.com/json-iterator/go"

	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	libp2p_crypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
	"github.com/multiformats/go-multiaddr"
	"google.golang.org/protobuf/proto"
)

const (
	StateRequestTopic      = "grapthway-state-request"
	StateResponseTopic     = "grapthway-state-response"
	DHTRequestTopic        = "grapthway-dht-request"
	DHTResponseTopic       = "grapthway-dht-response"
	LedgerUpdateTopic      = "grapthway-ledger-update"
	LedgerRequestTopic     = "grapthway-ledger-request"
	InstanceUpdateTopic    = "grapthway-instance-update"
	LedgerTransactionTopic = "grapthway-ledger-tx"
	HardwareStatsTopic     = "grapthway-hardware-stats"
	RecoveryProposalTopic  = "grapthway-recovery-proposal"
	LedgerMempoolAckTopic  = "grapthway-ledger-mempool-ack"
	BlockProposalTopic     = "grapthway-block-proposal"
	BlockShredTopic        = "grapthway-block-shred"
	PreValidatedTxTopic    = "grapthway-pre-validated-tx"
	NonceUpdateTopic       = "grapthway-nonce-update"
	BlockPreProposalTopic  = "grapthway-block-pre-proposal"
	ValidatorUpdateTopic   = "grapthway-validator-update"

	// Direct stream protocols for fast consensus communication
	PreBlockAckProtocolID    protocol.ID = "/grapthway/pre-block-ack/1.0.0"
	NextBlockReadyProtocolID protocol.ID = "/grapthway/next-block-ready/1.0.0"

	WorkflowProtocolID          protocol.ID = "/grapthway/workflow/1.0.0"
	LedgerSyncProtocolID        protocol.ID = "/grapthway/ledger-sync/1.0.0"
	HistorySyncProtocolID       protocol.ID = "/grapthway/history-sync/1.0.0"
	PeerExchangeProtocolID      protocol.ID = "/grapthway/peer-exchange/1.0.0"
	NodeHelloProtocolID         protocol.ID = "/grapthway/node-hello/1.0.0"
	FullSyncProtocolID          protocol.ID = "/grapthway/full-sync/1.0.0"
	MissingTxRequestProtocolID  protocol.ID = "/grapthway/missing-tx-req/1.0.0"
	ChainTipQuorumProtocolID    protocol.ID = "/grapthway/chain-tip-quorum/1.0.0"
	BlockRequestProtocolID      protocol.ID = "/grapthway/block-req/1.0.0"
	MempoolSyncProtocolID       protocol.ID = "/grapthway/mempool-sync/1.0.0"
	MissingTxsRequestProtocolID protocol.ID = "/grapthway/missing-txs-req/1.0.0"
	KnownTxsProtocolID          protocol.ID = "/grapthway/known-txs/1.0.0"

	peerLowWaterMark   = 2
	peerHighWaterMark  = 80
	connMgrGracePeriod = 2 * time.Minute
	denialPeriod       = 5 * time.Minute
	GrapthwayNetworkID = "grapthway-mainnet-v1"
	GrapthwayVersion   = "0.1.0"
)

type StakingInfoProvider interface {
	GetTotalStakeForNode(nodePeerID string) (uint64, error)
	GetFullSyncState() ([]byte, error)
	ApplyFullSyncState([]byte) error
	GetLatestBlockHash() string
	FindMempoolTxByNonce(address string, nonce uint64) (*types.Transaction, bool)
	GetLatestBlockHeight() uint64
	GetMempoolTxIDs() []string
	GetMempoolTxByID(txID string) (*types.Transaction, bool)
	HandleTransactionGossip([]byte)
	GetBlocksByRange(startHeight uint64, limit uint64) ([]types.Block, error)
	ProcessCatchupBlocks(blocks []types.Block) error
	GetMempoolTxsByIDs(txIDs []string) ([]types.Transaction, error)
	HandlePreBlockProposal(data []byte)
	HandleValidatorUpdate(data []byte)
	RevalidateValidators()
	GetValidatorSet() []types.ValidatorInfo
	HandlePeerConnectionEvent(peerID peer.ID)
}

type NodeHelloPayload struct {
	HardwareInfo model.HardwareInfo `json:"hardwareInfo"`
	OwnerAddress string             `json:"ownerAddress"`
	Version      string             `json:"version"`
	NetworkID    string             `json:"networkId"`
}

type HandshakeResponse struct {
	Success bool   `json:"success"`
	Reason  string `json:"reason,omitempty"`
}

type Node struct {
	Host                  host.Host
	PubSub                *pubsub.PubSub
	DHT                   *dht.IpfsDHT
	subscriptions         map[string]*pubsub.Subscription
	subMutex              sync.RWMutex
	ctx                   context.Context
	ReconciliationTrigger chan struct{}
	initialBootstrapPeers []string
	lastStateBroadcast    int64
	OwnerAddress          string
	Identity              *gwcrypto.Identity
	Role                  string

	stakingProvider        StakingInfoProvider
	hardwareMonitor        model.HardwareInfoProvider
	networkHardwareMonitor *monitoring.NetworkHardwareMonitor
	gcuPerCore             float64

	deniedPeers      map[peer.ID]time.Time
	deniedPeersMutex sync.Mutex
	isSynced         atomic.Bool
	syncMutex        sync.Mutex
	syncComplete     chan struct{}

	ackChannels      map[string]chan peer.ID
	ackChannelsMutex sync.Mutex

	preBlockAckChannels      map[string]chan model.PreBlockAck
	preBlockAckChannelsMutex sync.Mutex
	nextBlockReadyAcks       map[uint64]chan peer.ID
	nextBlockReadyAcksMutex  sync.Mutex

	receivedReadySignals      map[uint64]map[peer.ID]struct{}
	receivedReadySignalsMutex sync.RWMutex
}

func getP2PPort() int {
	if portStr := os.Getenv("P2P_PORT"); portStr != "" {
		if port, err := strconv.Atoi(portStr); err == nil && port > 0 {
			return port
		}
	}
	return 40949
}

func getListenAddr() string {
	if addr := os.Getenv("P2P_LISTEN_ADDR"); addr != "" {
		return addr
	}
	return "0.0.0.0"
}

func gossipSubParams() pubsub.GossipSubParams {
	numCPU := runtime.GOMAXPROCS(0)
	if numCPU == 0 {
		numCPU = 1
	}

	p := pubsub.DefaultGossipSubParams()

	// Scale the gossip mesh degree based on available CPUs.
	// A more powerful node can handle more connections, improving network health.
	// We'll cap the values to prevent excessive connections on very high-core machines.
	p.D = util.Clamp(8+(numCPU/2), 6, 24)   // Desired number of peers in mesh
	p.Dlo = util.Clamp(6+(numCPU/4), 4, 16) // Low watermark, below which we seek more peers
	p.Dhi = util.Clamp(12+numCPU, 10, 40)   // High watermark, above which we prune peers

	p.HeartbeatInterval = 700 * time.Millisecond
	p.HistoryGossip = 5
	p.HistoryLength = 10
	p.MaxIHaveLength = 10000

	log.Printf("⚙️  Configuring GossipSub mesh (D=%d, Dlo=%d, Dhi=%d) for %d CPUs", p.D, p.Dlo, p.Dhi, numCPU)
	return p
}

func NewNode(ctx context.Context, bootstrapPeers []string, privKey libp2p_crypto.PrivKey, ownerAddress string, stakingProvider StakingInfoProvider, hwMonitor model.HardwareInfoProvider, gcuPerCore float64, networkMonitor *monitoring.NetworkHardwareMonitor, identity *gwcrypto.Identity, role string) (*Node, error) {
	p2pPort := getP2PPort()
	listenAddr := getListenAddr()

	cm, err := connmgr.NewConnManager(
		peerLowWaterMark,
		peerHighWaterMark,
		connmgr.WithGracePeriod(connMgrGracePeriod),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection manager: %w", err)
	}
	log.Printf("P2P: Connection manager configured with low:%d, high:%d, grace:%v", peerLowWaterMark, peerHighWaterMark, connMgrGracePeriod)

	listenAddrs := []multiaddr.Multiaddr{}
	tcpAddr, err := multiaddr.NewMultiaddr(fmt.Sprintf("/ip4/%s/tcp/%d", listenAddr, p2pPort))
	if err != nil {
		return nil, fmt.Errorf("failed to create TCP multiaddr: %w", err)
	}
	listenAddrs = append(listenAddrs, tcpAddr)
	quicAddr, err := multiaddr.NewMultiaddr(fmt.Sprintf("/ip4/%s/udp/%d/quic-v1", listenAddr, p2pPort))
	if err != nil {
		log.Printf("WARN: Failed to create QUIC multiaddr: %v. Proceeding with TCP only.", err)
	} else {
		listenAddrs = append(listenAddrs, quicAddr)
	}

	var kadDHT *dht.IpfsDHT
	h, err := libp2p.New(
		libp2p.Identity(privKey),
		libp2p.ListenAddrs(listenAddrs...),
		libp2p.ConnectionManager(cm),
		libp2p.DisableRelay(),
		libp2p.Routing(func(h host.Host) (routing.PeerRouting, error) {
			var dhtErr error
			kadDHT, dhtErr = dht.New(ctx, h)
			return kadDHT, dhtErr
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create libp2p host: %w", err)
	}

	ps, err := pubsub.NewGossipSub(ctx, h,
		pubsub.WithPeerScore(peerScoreParams(), peerScoreThresholds()),
		pubsub.WithGossipSubParams(gossipSubParams()),
		pubsub.WithSeenMessagesTTL(2*time.Minute),
		pubsub.WithFloodPublish(true),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create pubsub service: %w", err)
	}

	log.Printf("P2P Node started with ID: %s", h.ID().String())
	log.Printf("P2P Node configured to listen on port: %d", p2pPort)
	for _, addr := range h.Addrs() {
		log.Printf("Node listening on address: %s/p2p/%s", addr.String(), h.ID().String())
	}

	node := &Node{
		Host:                   h,
		PubSub:                 ps,
		DHT:                    kadDHT,
		subscriptions:          make(map[string]*pubsub.Subscription),
		ctx:                    ctx,
		ReconciliationTrigger:  make(chan struct{}, 1),
		initialBootstrapPeers:  bootstrapPeers,
		OwnerAddress:           ownerAddress,
		Identity:               identity,
		Role:                   role,
		stakingProvider:        stakingProvider,
		hardwareMonitor:        hwMonitor,
		networkHardwareMonitor: networkMonitor,
		gcuPerCore:             gcuPerCore,
		deniedPeers:            make(map[peer.ID]time.Time),
		syncComplete:           make(chan struct{}),
		ackChannels:            make(map[string]chan peer.ID),
		preBlockAckChannels:    make(map[string]chan model.PreBlockAck),
		nextBlockReadyAcks:     make(map[uint64]chan peer.ID),
		receivedReadySignals:   make(map[uint64]map[peer.ID]struct{}),
	}

	h.SetStreamHandler(PeerExchangeProtocolID, node.handlePeerExchangeStream)
	h.SetStreamHandler(NodeHelloProtocolID, node.handleNodeHelloStream)
	h.SetStreamHandler(FullSyncProtocolID, node.handleFullSyncStream)
	h.SetStreamHandler(MissingTxRequestProtocolID, node.handleMissingTxRequestStream)
	h.SetStreamHandler(ChainTipQuorumProtocolID, node.handleChainTipQuorumStream)
	h.SetStreamHandler(BlockRequestProtocolID, node.handleBlockRequestStream)
	h.SetStreamHandler(MempoolSyncProtocolID, node.handleMempoolSyncStream)
	h.SetStreamHandler(MissingTxsRequestProtocolID, node.handleMissingTxsRequestStream)
	h.SetStreamHandler(KnownTxsProtocolID, node.handleKnownTxsRequestStream)

	if role != "worker" {
		h.SetStreamHandler(PreBlockAckProtocolID, node.handlePreBlockAckStream)
		h.SetStreamHandler(NextBlockReadyProtocolID, node.handleNextBlockReadyStream)
	}

	go node.subscribeAndProcessAcks(ctx)
	go node.bootstrapConnect(node.initialBootstrapPeers)
	go node.maintainNetworkConnectivity()
	go node.listenForConnectionEvents()
	go node.cleanupDeniedPeers()

	isWorker := role == "worker"
	if len(bootstrapPeers) == 0 || isWorker {
		log.Println("P2P: This node is a worker or the first node. Marking as synced for messaging.")
		node.isSynced.Store(true)
		close(node.syncComplete)
	}

	return node, nil
}

// handlePreBlockAckStream handles incoming signed acknowledgements for a pre-block proposal.
func (n *Node) handlePreBlockAckStream(s network.Stream) {
	if n.Role == "worker" {
		s.Reset()
		return
	}
	defer s.Close()
	remotePeer := s.Conn().RemotePeer()

	data, err := io.ReadAll(s)
	if err != nil {
		log.Printf("PRE-BLOCK-ACK: Failed to read ACK data from %s: %v", remotePeer, err)
		s.Reset()
		return
	}
	var pbAck pb.PreBlockAck
	if err := proto.Unmarshal(data, &pbAck); err != nil {
		log.Printf("PRE-BLOCK-ACK: Failed to decode ACK from %s: %v", remotePeer, err)
		s.Reset()
		return
	}
	ack := prototomodel.ProtoToModelPreBlockAck(&pbAck)

	n.preBlockAckChannelsMutex.Lock()
	ch, exists := n.preBlockAckChannels[ack.ProposalID]
	n.preBlockAckChannelsMutex.Unlock()

	if exists {
		select {
		case ch <- *ack:
		default:
			log.Printf("PRE-BLOCK-ACK: Ack channel for proposal %s is full or closed.", ack.ProposalID)
		}
	}
}

// handleNextBlockReadyStream handles signals from validators that they are ready for the next block.
func (n *Node) handleNextBlockReadyStream(s network.Stream) {
	if n.Role == "worker" {
		s.Reset()
		return
	}
	defer s.Close()
	remotePeer := s.Conn().RemotePeer()

	data, err := io.ReadAll(s)
	if err != nil {
		log.Printf("NEXT-BLOCK-READY: Failed to read message from %s: %v", remotePeer, err)
		s.Reset()
		return
	}
	var pbReadyMsg pb.NextBlockReady
	if err := proto.Unmarshal(data, &pbReadyMsg); err != nil {
		log.Printf("NEXT-BLOCK-READY: Failed to decode message from %s: %v", remotePeer, err)
		s.Reset()
		return
	}
	readyMsg := prototomodel.ProtoToModelNextBlockReady(&pbReadyMsg)

	validatorPID, err := peer.Decode(readyMsg.ValidatorID)
	if err != nil {
		log.Printf("NEXT-BLOCK-READY: Invalid validator ID '%s' from %s", readyMsg.ValidatorID, remotePeer)
		return
	}

	n.receivedReadySignalsMutex.Lock()
	if _, ok := n.receivedReadySignals[readyMsg.BlockHeight]; !ok {
		n.receivedReadySignals[readyMsg.BlockHeight] = make(map[peer.ID]struct{})
	}
	n.receivedReadySignals[readyMsg.BlockHeight][validatorPID] = struct{}{}
	n.receivedReadySignalsMutex.Unlock()

	n.nextBlockReadyAcksMutex.Lock()
	ch, exists := n.nextBlockReadyAcks[readyMsg.BlockHeight]
	n.nextBlockReadyAcksMutex.Unlock()

	if exists {
		select {
		case ch <- validatorPID:
		case <-n.ctx.Done():
		default:
			log.Printf("NEXT-BLOCK-READY: Could not send signal to live channel for height %d from %s, channel is full or context is done.", readyMsg.BlockHeight, remotePeer)
		}
	} else {
		log.Printf("NEXT-BLOCK-READY: Received and cached signal for height %d from %s, but no listening channel was found.", readyMsg.BlockHeight, remotePeer)
	}
}

func (n *Node) PrepareForNextBlock(height uint64) {
	n.nextBlockReadyAcksMutex.Lock()
	defer n.nextBlockReadyAcksMutex.Unlock()

	if _, exists := n.nextBlockReadyAcks[height]; !exists {
		n.nextBlockReadyAcks[height] = make(chan peer.ID, 100)
		log.Printf("P2P-READY: Prepared listening channel for block height %d", height)
	}
}

func (n *Node) CleanUpReadyChannel(height uint64) {
	n.nextBlockReadyAcksMutex.Lock()
	if ch, exists := n.nextBlockReadyAcks[height]; exists {
		close(ch)
		delete(n.nextBlockReadyAcks, height)
	}
	n.nextBlockReadyAcksMutex.Unlock()

	n.receivedReadySignalsMutex.Lock()
	delete(n.receivedReadySignals, height)
	n.receivedReadySignalsMutex.Unlock()

	log.Printf("P2P-READY: Cleaned up channel and cached signals for processed block height %d", height)
}

func (n *Node) SendNextBlockReady(ctx context.Context, leaderID peer.ID, height uint64) error {
	s, err := n.Host.NewStream(ctx, leaderID, NextBlockReadyProtocolID)
	if err != nil {
		return fmt.Errorf("failed to open next-block-ready stream to %s: %w", leaderID, err)
	}
	defer s.Close()
	s.SetDeadline(time.Now().Add(2 * time.Second))

	msg := model.NextBlockReady{
		BlockHeight: height,
		ValidatorID: n.Host.ID().String(),
	}
	pbMsg := modeltoproto.ModelToProtoNextBlockReady(&msg)
	data, err := proto.Marshal(pbMsg)
	if err != nil {
		s.Reset()
		return fmt.Errorf("failed to marshal next-block-ready message: %w", err)
	}

	if _, err := s.Write(data); err != nil {
		s.Reset()
		return fmt.Errorf("failed to send next-block-ready to %s: %w", leaderID, err)
	}
	return nil
}

func (n *Node) CollectNextBlockReady(ctx context.Context, height uint64, requiredCount int) error {
	if requiredCount <= 0 {
		return nil
	}

	n.nextBlockReadyAcksMutex.Lock()
	ackChan, exists := n.nextBlockReadyAcks[height]
	if !exists {
		log.Printf("LEADER: WARNING: 'Ready' signal channel for height %d was not pre-created. Creating it now.", height)
		ackChan = make(chan peer.ID, requiredCount*2)
		n.nextBlockReadyAcks[height] = ackChan
	}
	n.nextBlockReadyAcksMutex.Unlock()

	receivedAcks := make(map[peer.ID]struct{})
	receivedAcks[n.Host.ID()] = struct{}{}

	n.receivedReadySignalsMutex.RLock()
	if signals, ok := n.receivedReadySignals[height]; ok {
		for peerID := range signals {
			receivedAcks[peerID] = struct{}{}
		}
	}
	n.receivedReadySignalsMutex.RUnlock()

	stillNeeded := requiredCount - len(receivedAcks)
	if stillNeeded <= 0 {
		log.Printf("LEADER: 'Ready' signal quorum met from cache for height %d (%d/%d).", height, len(receivedAcks), requiredCount)
		return nil
	}

	log.Printf("LEADER: Waiting for %d more 'Ready' signals for height %d... (already have %d/%d from cache)", stillNeeded, height, len(receivedAcks), requiredCount)

	for {
		if len(receivedAcks) >= requiredCount {
			log.Printf("LEADER: 'Ready' signal quorum reached for height %d (%d/%d).", height, len(receivedAcks), requiredCount)
			return nil
		}

		select {
		case peerID := <-ackChan:
			if _, exists := receivedAcks[peerID]; !exists {
				receivedAcks[peerID] = struct{}{}
				stillNeeded = requiredCount - len(receivedAcks)
				if stillNeeded < 0 {
					stillNeeded = 0
				}
				log.Printf("LEADER: Received live signal. Waiting for %d more 'Ready' signals for height %d... (have %d/%d)", stillNeeded, height, len(receivedAcks), requiredCount)
			}
		case <-ctx.Done():
			return fmt.Errorf("context canceled while waiting for 'Ready' signals for height %d (received %d/%d)", height, len(receivedAcks), requiredCount)
		}
	}
}

func (n *Node) SendPreBlockAck(ctx context.Context, leaderID peer.ID, ack model.PreBlockAck) error {
	s, err := n.Host.NewStream(ctx, leaderID, PreBlockAckProtocolID)
	if err != nil {
		return fmt.Errorf("failed to open pre-block ack stream to %s: %w", leaderID, err)
	}
	defer s.Close()
	s.SetDeadline(time.Now().Add(2 * time.Second))

	pbAck := modeltoproto.ModelToProtoPreBlockAck(&ack)
	data, err := proto.Marshal(pbAck)
	if err != nil {
		s.Reset()
		return fmt.Errorf("failed to marshal pre-block ack for %s: %w", leaderID, err)
	}

	if _, err := s.Write(data); err != nil {
		s.Reset()
		return fmt.Errorf("failed to send pre-block ack to %s: %w", leaderID, err)
	}
	return nil
}

func (n *Node) CollectPreBlockAcks(ctx context.Context, proposalID string) <-chan model.PreBlockAck {
	ackChan := make(chan model.PreBlockAck, len(n.GetPeerList()))
	n.preBlockAckChannelsMutex.Lock()
	n.preBlockAckChannels[proposalID] = ackChan
	n.preBlockAckChannelsMutex.Unlock()

	go func() {
		<-ctx.Done()
		n.preBlockAckChannelsMutex.Lock()
		delete(n.preBlockAckChannels, proposalID)
		n.preBlockAckChannelsMutex.Unlock()
		close(ackChan)
	}()

	return ackChan
}

func (n *Node) handleKnownTxsRequestStream(s network.Stream) {
	defer s.Close()
	data, err := io.ReadAll(s)
	if err != nil {
		s.Reset()
		return
	}

	var pbReq pb.KnownTxsRequest
	if err := proto.Unmarshal(data, &pbReq); err != nil {
		log.Printf("KNOWN-TXS: Failed to decode request from %s: %v", s.Conn().RemotePeer(), err)
		s.Reset()
		return
	}

	pbResp := &pb.KnownTxsResponse{KnownTxIds: make(map[string]bool)}
	for _, txID := range pbReq.TxIds {
		if _, found := n.stakingProvider.GetMempoolTxByID(txID); found {
			pbResp.KnownTxIds[txID] = true
		}
	}

	respData, err := proto.Marshal(pbResp)
	if err != nil {
		log.Printf("KNOWN-TXS: Failed to marshal response for %s: %v", s.Conn().RemotePeer(), err)
		s.Reset()
		return
	}

	if _, err := s.Write(respData); err != nil {
		log.Printf("KNOWN-TXS: Failed to send response to %s: %v", s.Conn().RemotePeer(), err)
		s.Reset()
	}
}

func (n *Node) RequestPeerKnownTxs(ctx context.Context, peerID peer.ID, txIDs []string) (map[string]struct{}, error) {
	s, err := n.Host.NewStream(ctx, peerID, KnownTxsProtocolID)
	if err != nil {
		return nil, fmt.Errorf("failed to open known txs stream to %s: %w", peerID, err)
	}
	defer s.Close()
	s.SetDeadline(time.Now().Add(2 * time.Second))

	pbReq := &pb.KnownTxsRequest{TxIds: txIDs}
	reqData, err := proto.Marshal(pbReq)
	if err != nil {
		s.Reset()
		return nil, fmt.Errorf("failed to marshal known txs request for %s: %w", peerID, err)
	}

	if _, err := s.Write(reqData); err != nil {
		s.Reset()
		return nil, fmt.Errorf("failed to send known txs request to %s: %w", peerID, err)
	}
	s.CloseWrite()

	respData, err := io.ReadAll(s)
	if err != nil {
		s.Reset()
		return nil, fmt.Errorf("failed to read known txs response from %s: %w", peerID, err)
	}

	var pbResp pb.KnownTxsResponse
	if err := proto.Unmarshal(respData, &pbResp); err != nil {
		s.Reset()
		return nil, fmt.Errorf("failed to decode known txs response from %s: %w", peerID, err)
	}

	knownSet := make(map[string]struct{})
	for id := range pbResp.KnownTxIds {
		knownSet[id] = struct{}{}
	}
	return knownSet, nil
}

func (n *Node) handleMempoolSyncStream(s network.Stream) {
	defer s.Close()
	remotePeer := s.Conn().RemotePeer()

	data, err := io.ReadAll(s)
	if err != nil {
		s.Reset()
		return
	}
	var pbReq pb.MempoolSyncRequest
	if err := proto.Unmarshal(data, &pbReq); err != nil {
		log.Printf("MEMPOOL-SYNC: Failed to decode request from %s: %v", remotePeer, err)
		s.Reset()
		return
	}

	var missingTxs []*pb.Transaction
	allLocalTxIDs := n.stakingProvider.GetMempoolTxIDs()
	for _, txID := range allLocalTxIDs {
		if _, known := pbReq.KnownTxIds[txID]; !known {
			if tx, found := n.stakingProvider.GetMempoolTxByID(txID); found {
				missingTxs = append(missingTxs, modeltoproto.ModelToProtoTransaction(tx))
			}
		}
	}

	if len(missingTxs) > 0 {
		pbResp := &pb.MempoolSyncResponse{MissingTxs: missingTxs}
		respData, err := proto.Marshal(pbResp)
		if err != nil {
			s.Reset()
			return
		}
		if _, err := s.Write(respData); err != nil {
			log.Printf("MEMPOOL-SYNC: Failed to send response to %s: %v", remotePeer, err)
			s.Reset()
		}
	}
}

func (n *Node) SynchronizeMempool(ctx context.Context, knownTxIDs map[string]struct{}) ([]types.Transaction, error) {
	peers := n.GetPeerList()
	if len(peers) == 0 {
		return nil, nil
	}

	var wg sync.WaitGroup
	resultsChan := make(chan []*pb.Transaction, len(peers))
	knownMap := make(map[string]bool, len(knownTxIDs))
	for id := range knownTxIDs {
		knownMap[id] = true
	}
	pbReq := &pb.MempoolSyncRequest{KnownTxIds: knownMap}
	reqData, err := proto.Marshal(pbReq)
	if err != nil {
		return nil, err
	}

	for _, p := range peers {
		wg.Add(1)
		go func(peerID peer.ID) {
			defer wg.Done()
			s, err := n.Host.NewStream(ctx, peerID, MempoolSyncProtocolID)
			if err != nil {
				return
			}
			defer s.Close()
			s.SetDeadline(time.Now().Add(5 * time.Second))

			if _, err := s.Write(reqData); err != nil {
				s.Reset()
				return
			}
			s.CloseWrite()

			respData, err := io.ReadAll(s)
			if err != nil || len(respData) == 0 {
				s.Reset()
				return
			}

			var pbResp pb.MempoolSyncResponse
			if err := proto.Unmarshal(respData, &pbResp); err != nil {
				s.Reset()
				return
			}

			if len(pbResp.MissingTxs) > 0 {
				resultsChan <- pbResp.MissingTxs
			}
		}(p)
	}

	wg.Wait()
	close(resultsChan)

	var newTxs []types.Transaction
	seenTxIDs := make(map[string]struct{})

	for txs := range resultsChan {
		for _, tx := range txs {
			if _, seen := seenTxIDs[tx.Id]; !seen {
				newTxs = append(newTxs, *prototomodel.ProtoToModelTransaction(tx))
				seenTxIDs[tx.Id] = struct{}{}
			}
		}
	}

	return newTxs, nil
}

func (n *Node) handleMissingTxsRequestStream(s network.Stream) {
	defer s.Close()
	remotePeer := s.Conn().RemotePeer()

	data, err := io.ReadAll(s)
	if err != nil {
		s.Reset()
		return
	}

	var pbReq pb.MissingTxsRequest
	if err := proto.Unmarshal(data, &pbReq); err != nil {
		log.Printf("MISSING-TXS-REQ: Failed to decode request from %s: %v", remotePeer, err)
		s.Reset()
		return
	}

	foundTxs, err := n.stakingProvider.GetMempoolTxsByIDs(pbReq.TxIds)
	if err != nil {
		log.Printf("MISSING-TXS-REQ: Failed to get requested transactions from ledger: %v", err)
		s.Reset()
		return
	}

	pbTxs := make([]*pb.Transaction, len(foundTxs))
	for i, tx := range foundTxs {
		pbTxs[i] = modeltoproto.ModelToProtoTransaction(&tx)
	}
	pbResp := &pb.MissingTxsResponse{MissingTxs: pbTxs}
	respData, err := proto.Marshal(pbResp)
	if err != nil {
		s.Reset()
		return
	}

	if _, err := s.Write(respData); err != nil {
		log.Printf("MISSING-TXS-REQ: Failed to send response to %s: %v", remotePeer, err)
		s.Reset()
	}
}

func (n *Node) RequestMissingTxs(ctx context.Context, peerID peer.ID, txIDs []string) ([]types.Transaction, error) {
	s, err := n.Host.NewStream(ctx, peerID, MissingTxsRequestProtocolID)
	if err != nil {
		return nil, fmt.Errorf("failed to open missing txs request stream to %s: %w", peerID, err)
	}
	defer s.Close()
	s.SetDeadline(time.Now().Add(3 * time.Second))

	pbReq := &pb.MissingTxsRequest{TxIds: txIDs}
	reqData, err := proto.Marshal(pbReq)
	if err != nil {
		s.Reset()
		return nil, err
	}
	if _, err := s.Write(reqData); err != nil {
		s.Reset()
		return nil, fmt.Errorf("failed to send missing txs request to %s: %w", peerID, err)
	}
	s.CloseWrite()

	respData, err := io.ReadAll(s)
	if err != nil {
		s.Reset()
		return nil, fmt.Errorf("failed to read missing txs response from %s: %w", peerID, err)
	}

	var pbResp pb.MissingTxsResponse
	if err := proto.Unmarshal(respData, &pbResp); err != nil {
		s.Reset()
		return nil, fmt.Errorf("failed to decode missing txs response from %s: %w", peerID, err)
	}

	modelTxs := make([]types.Transaction, len(pbResp.MissingTxs))
	for i, tx := range pbResp.MissingTxs {
		modelTxs[i] = *prototomodel.ProtoToModelTransaction(tx)
	}

	return modelTxs, nil
}

func (n *Node) handleBlockRequestStream(s network.Stream) {
	defer s.Close()
	remotePeer := s.Conn().RemotePeer()

	data, err := io.ReadAll(s)
	if err != nil {
		s.Reset()
		return
	}

	var pbReq pb.BlockRequest
	if err := proto.Unmarshal(data, &pbReq); err != nil {
		log.Printf("BLOCK-SYNC: Failed to decode block request from %s: %v", remotePeer, err)
		s.Reset()
		return
	}

	log.Printf("BLOCK-SYNC: Received request for blocks starting from height %d from peer %s", pbReq.StartHeight, remotePeer)

	blocks, err := n.stakingProvider.GetBlocksByRange(pbReq.StartHeight, pbReq.Limit)
	if err != nil {
		log.Printf("BLOCK-SYNC: Error retrieving blocks for peer %s: %v", remotePeer, err)
		s.Reset()
		return
	}

	pbBlocks := make([]*pb.Block, len(blocks))
	for i, b := range blocks {
		pbBlocks[i] = modeltoproto.ModelToProtoBlock(&b)
	}
	pbResp := &pb.BlockResponse{Blocks: pbBlocks}
	respData, err := proto.Marshal(pbResp)
	if err != nil {
		s.Reset()
		return
	}

	if _, err := s.Write(respData); err != nil {
		log.Printf("BLOCK-SYNC: Failed to send block response to %s: %v", remotePeer, err)
		s.Reset()
	}

	log.Printf("BLOCK-SYNC: Sent %d blocks to peer %s", len(blocks), remotePeer)
}

func (n *Node) RequestMissingBlocks(ctx context.Context, peerID peer.ID, req types.BlockRequest) ([]types.Block, error) {
	s, err := n.Host.NewStream(ctx, peerID, BlockRequestProtocolID)
	if err != nil {
		return nil, fmt.Errorf("failed to open block request stream to %s: %w", peerID, err)
	}
	defer s.Close()
	s.SetDeadline(time.Now().Add(10 * time.Second))

	pbReq := &pb.BlockRequest{StartHeight: req.StartHeight, Limit: req.Limit}
	reqData, err := proto.Marshal(pbReq)
	if err != nil {
		s.Reset()
		return nil, err
	}
	if _, err := s.Write(reqData); err != nil {
		s.Reset()
		return nil, fmt.Errorf("failed to send block request to %s: %w", peerID, err)
	}
	s.CloseWrite()

	respData, err := io.ReadAll(s)
	if err != nil {
		s.Reset()
		return nil, err
	}
	var pbResp pb.BlockResponse
	if err := proto.Unmarshal(respData, &pbResp); err != nil {
		s.Reset()
		return nil, fmt.Errorf("failed to decode block response from %s: %w", peerID, err)
	}

	modelBlocks := make([]types.Block, len(pbResp.Blocks))
	for i, b := range pbResp.Blocks {
		modelBlocks[i] = *prototomodel.ProtoToModelBlock(b)
	}

	return modelBlocks, nil
}

func (n *Node) handleChainTipQuorumStream(s network.Stream) {
	defer s.Close()

	if !n.isSynced.Load() {
		log.Printf("QUORUM: Ignoring chain tip request from %s because this node is not synced.", s.Conn().RemotePeer())
		return
	}

	pbResp := &pb.ChainTipQuorumCheck{
		BlockHash:   n.stakingProvider.GetLatestBlockHash(),
		BlockHeight: n.stakingProvider.GetLatestBlockHeight(),
	}
	respData, err := proto.Marshal(pbResp)
	if err != nil {
		s.Reset()
		return
	}
	if _, err := s.Write(respData); err != nil {
		log.Printf("QUORUM: Failed to send chain tip response to %s: %v", s.Conn().RemotePeer(), err)
		s.Reset()
	}
}

func (n *Node) handleMissingTxRequestStream(s network.Stream) {
	defer s.Close()
	data, err := io.ReadAll(s)
	if err != nil {
		s.Reset()
		return
	}
	var pbReq pb.MissingTxRequestByNonce
	if err := proto.Unmarshal(data, &pbReq); err != nil {
		log.Printf("NETWORK-HEAL: Failed to decode missing tx request: %v", err)
		s.Reset()
		return
	}

	if tx, found := n.stakingProvider.FindMempoolTxByNonce(pbReq.Address, pbReq.Nonce); found {
		pbTx := modeltoproto.ModelToProtoTransaction(tx)
		txData, err := proto.Marshal(pbTx)
		if err != nil {
			s.Reset()
			return
		}
		if _, err := s.Write(txData); err != nil {
			log.Printf("NETWORK-HEAL: Failed to encode and send found tx %s: %v", tx.ID, err)
			s.Reset()
		}
	}
}

func (n *Node) RequestMissingTx(ctx context.Context, address string, nonce uint64) (*types.Transaction, error) {
	req := model.MissingTxRequest{Address: address, Nonce: nonce}
	peers := n.GetPeerList()
	if len(peers) == 0 {
		return nil, errors.New("no connected peers to ask")
	}

	resultChan := make(chan *types.Transaction, 1)
	queryCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	for _, p := range peers {
		wg.Add(1)
		go func(peerID peer.ID) {
			defer wg.Done()
			tx, err := n.requestTxFromPeer(queryCtx, peerID, req)
			if err == nil && tx != nil {
				select {
				case resultChan <- tx:
					cancel()
				case <-queryCtx.Done():
				}
			}
		}(p)
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	select {
	case tx := <-resultChan:
		if tx != nil {
			return tx, nil
		}
		return nil, errors.New("transaction not found on network")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (n *Node) requestTxFromPeer(ctx context.Context, peerID peer.ID, req model.MissingTxRequest) (*types.Transaction, error) {
	s, err := n.Host.NewStream(ctx, peerID, MissingTxRequestProtocolID)
	if err != nil {
		return nil, err
	}
	defer s.Close()
	s.SetDeadline(time.Now().Add(2 * time.Second))

	pbReq := &pb.MissingTxRequestByNonce{Address: req.Address, Nonce: req.Nonce}
	reqData, err := proto.Marshal(pbReq)
	if err != nil {
		s.Reset()
		return nil, err
	}
	if _, err := s.Write(reqData); err != nil {
		s.Reset()
		return nil, err
	}
	s.CloseWrite()

	respData, err := io.ReadAll(s)
	if err != nil {
		if err == io.EOF {
			return nil, nil
		}
		return nil, err
	}
	if len(respData) == 0 {
		return nil, nil
	}

	var pbTx pb.Transaction
	if err := proto.Unmarshal(respData, &pbTx); err != nil {
		return nil, err
	}

	receivedTx := prototomodel.ProtoToModelTransaction(&pbTx)
	if receivedTx.From == req.Address && receivedTx.Nonce == req.Nonce {
		return receivedTx, nil
	}

	return nil, errors.New("received transaction did not match request")
}

func peerScoreParams() *pubsub.PeerScoreParams {
	return &pubsub.PeerScoreParams{
		Topics:                      make(map[string]*pubsub.TopicScoreParams),
		AppSpecificScore:            func(p peer.ID) float64 { return 1.0 },
		AppSpecificWeight:           1,
		IPColocationFactorWeight:    -10,
		IPColocationFactorThreshold: 2,
		BehaviourPenaltyWeight:      -10,
		BehaviourPenaltyThreshold:   6,
		BehaviourPenaltyDecay:       pubsub.ScoreParameterDecay(time.Hour),
		DecayInterval:               pubsub.DefaultDecayInterval,
		DecayToZero:                 pubsub.DefaultDecayToZero,
		RetainScore:                 12 * time.Hour,
	}
}

func peerScoreThresholds() *pubsub.PeerScoreThresholds {
	return &pubsub.PeerScoreThresholds{
		GossipThreshold:             -10,
		PublishThreshold:            -50,
		GraylistThreshold:           -100,
		AcceptPXThreshold:           10,
		OpportunisticGraftThreshold: 20,
	}
}

func (n *Node) RequestChainTipQuorum(ctx context.Context, leaderHash string, leaderHeight uint64) (majorityHash string, majorityHeight uint64, votesForMajority int, totalVotes int, err error) {
	peers := n.GetPeerList()
	responses := make(chan *pb.ChainTipQuorumCheck, len(peers))
	var wg sync.WaitGroup

	for _, p := range peers {
		wg.Add(1)
		go func(peerID peer.ID) {
			defer wg.Done()
			s, err := n.Host.NewStream(ctx, peerID, ChainTipQuorumProtocolID)
			if err != nil {
				return
			}
			defer s.Close()
			s.SetDeadline(time.Now().Add(3 * time.Second))

			data, err := io.ReadAll(s)
			if err != nil {
				s.Reset()
				return
			}
			var resp pb.ChainTipQuorumCheck
			if err := proto.Unmarshal(data, &resp); err != nil {
				s.Reset()
				return
			}
			responses <- &resp
		}(p)
	}

	wg.Wait()
	close(responses)

	counts := make(map[string]int)
	heights := make(map[string]uint64)
	counts[leaderHash]++
	heights[leaderHash] = leaderHeight

	for resp := range responses {
		counts[resp.BlockHash]++
		heights[resp.BlockHash] = resp.BlockHeight
	}

	maxCount := 0
	for hash, count := range counts {
		if count > maxCount {
			maxCount = count
			majorityHash = hash
		}
	}

	majorityHeight = heights[majorityHash]
	votesForMajority = maxCount
	totalVotes = 0
	for _, count := range counts {
		totalVotes += count
	}

	return
}

func (n *Node) TriggerSyncFromRandomPeer() {
	if !n.isSynced.CompareAndSwap(true, false) {
		log.Println("SYNC_TRIGGER: A sync is already in progress. Ignoring request.")
		return
	}

	peers := n.GetPeerList()
	if len(peers) == 0 {
		log.Println("SYNC_TRIGGER: No peers to sync from. Reverting sync status.")
		n.isSynced.Store(true)
		return
	}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	randomPeer := peers[r.Intn(len(peers))]

	log.Printf("SYNC_TRIGGER: Proactively triggering full state sync from random peer %s", randomPeer)
	go n.requestFullSyncFromPeer(randomPeer)
}

func (n *Node) requestFullSyncFromPeer(p peer.ID) {
	log.Printf("SYNC: Requesting full state sync from peer %s", p)
	s, err := n.Host.NewStream(n.ctx, p, FullSyncProtocolID)
	if err != nil {
		log.Printf("SYNC: Failed to open full sync stream to %s: %v", p, err)
		if !n.isSynced.Load() {
			log.Println("SYNC: Re-enabling sync status after failed stream creation.")
			n.isSynced.Store(true)
		}
		return
	}
	n.handleFullSyncStream(s)
}

func (n *Node) handleFullSyncStream(s network.Stream) {
	defer s.Close()
	remotePeer := s.Conn().RemotePeer()

	if s.Stat().Direction == network.DirInbound {
		log.Printf("SYNC: Receiving sync request from peer %s. Sending our full state.", remotePeer)
		fullState, err := n.stakingProvider.GetFullSyncState()
		if err != nil {
			log.Printf("SYNC: CRITICAL: Failed to get full state for peer %s: %v", remotePeer, err)
			s.Reset()
			return
		}
		writer := bufio.NewWriter(s)
		if _, err := writer.Write(fullState); err != nil {
			log.Printf("SYNC: Failed to write full state to peer %s: %v", remotePeer, err)
			s.Reset()
			return
		}
		if err := writer.Flush(); err != nil {
			log.Printf("SYNC: Failed to flush full state to peer %s: %v", remotePeer, err)
			s.Reset()
			return
		}
	} else {
		log.Printf("SYNC: Receiving full state sync data from peer %s", remotePeer)
		data, err := io.ReadAll(s)
		if err != nil {
			log.Printf("SYNC: Error reading from full sync stream from %s: %v", remotePeer, err)
			s.Reset()
			return
		}

		if len(data) == 0 {
			log.Printf("SYNC: Received empty sync payload from %s. Ignoring.", remotePeer)
			if !n.isSynced.Load() {
				n.isSynced.Store(true)
			}
			return
		}

		if err := n.stakingProvider.ApplyFullSyncState(data); err != nil {
			log.Printf("SYNC: CRITICAL: Failed to apply full sync state from peer %s: %v", remotePeer, err)
			if !n.isSynced.Load() {
				n.isSynced.Store(true)
			}
			return
		}

		wasSynced := n.isSynced.Load()
		log.Println("SYNC: Full state sync applied successfully. Node is now operational.")
		n.isSynced.Store(true)

		if !wasSynced {
			n.syncMutex.Lock()
			select {
			case <-n.syncComplete:
			default:
				close(n.syncComplete)
			}
			n.syncMutex.Unlock()
		}
	}
}

func (n *Node) addDeniedPeer(p peer.ID) {
	n.deniedPeersMutex.Lock()
	defer n.deniedPeersMutex.Unlock()
	n.deniedPeers[p] = time.Now()
}

func (n *Node) isPeerDenied(p peer.ID) bool {
	n.deniedPeersMutex.Lock()
	defer n.deniedPeersMutex.Unlock()
	denialTime, exists := n.deniedPeers[p]
	if !exists {
		return false
	}
	if time.Since(denialTime) > denialPeriod {
		delete(n.deniedPeers, p)
		return false
	}
	return true
}

func (n *Node) cleanupDeniedPeers() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
			n.deniedPeersMutex.Lock()
			for p, denialTime := range n.deniedPeers {
				if time.Since(denialTime) > denialPeriod {
					delete(n.deniedPeers, p)
				}
			}
			n.deniedPeersMutex.Unlock()
		}
	}
}

func (n *Node) handleNodeHelloStream(s network.Stream) {
	defer s.Close()
	peerID := s.Conn().RemotePeer()
	log.Printf("HANDSHAKE: Received node-hello from %s", peerID)

	var payload NodeHelloPayload
	if err := json.NewDecoder(s).Decode(&payload); err != nil {
		log.Printf("HANDSHAKE: Failed to decode hello payload from %s: %v", peerID, err)
		s.Reset()
		return
	}

	sendRejection := func(reason string) {
		log.Printf("HANDSHAKE: REJECTED: %s. Reason: %s. Dropping connection.", peerID, reason)
		n.addDeniedPeer(peerID)
		response := HandshakeResponse{Success: false, Reason: reason}
		json.NewEncoder(s).Encode(response)
		s.Conn().Close()
	}

	if payload.NetworkID != GrapthwayNetworkID {
		sendRejection(fmt.Sprintf("Invalid Network ID. Expected %s, got %s", GrapthwayNetworkID, payload.NetworkID))
		return
	}
	if payload.Version != GrapthwayVersion {
		log.Printf("HANDSHAKE: WARNING: Peer %s has a different version (%s) than ours (%s).", peerID, payload.Version, GrapthwayVersion)
	}

	if payload.HardwareInfo.Role == "worker" {
		log.Printf("HANDSHAKE: Peer %s identified as a worker. Accepting connection without stake check.", peerID)
		response := HandshakeResponse{Success: true}
		if err := json.NewEncoder(s).Encode(&response); err != nil {
			log.Printf("HANDSHAKE: Failed to send success response to worker %s: %v", peerID, err)
			s.Reset()
		}
		return
	}

	if payload.HardwareInfo.CgroupLimits.CPUQuotaCores <= 0 {
		sendRejection(fmt.Sprintf("Peer has invalid CPU resources (%.2f cores)", payload.HardwareInfo.CgroupLimits.CPUQuotaCores))
		return
	}

	requiredStakeFloat := payload.HardwareInfo.CgroupLimits.CPUQuotaCores * n.gcuPerCore
	requiredStakeMicro := uint64(requiredStakeFloat * model.GCU_MICRO_UNIT)

	allocatedStakeMicro, err := n.stakingProvider.GetTotalStakeForNode(peerID.String())
	if err != nil {
		sendRejection(fmt.Sprintf("Could not verify stake: %v", err))
		return
	}

	allocatedStakeFloat := float64(allocatedStakeMicro) / model.GCU_MICRO_UNIT
	log.Printf("HANDSHAKE: Verifying peer %s. Requires: %.4f GCU, Has: %.4f GCU.", peerID, requiredStakeFloat, allocatedStakeFloat)

	if allocatedStakeMicro < requiredStakeMicro {
		sendRejection(fmt.Sprintf("Insufficient stake. Required %.4f, but only %.4f is allocated.", requiredStakeFloat, allocatedStakeFloat))
		return
	}

	response := HandshakeResponse{Success: true}
	if err := json.NewEncoder(s).Encode(&response); err != nil {
		log.Printf("HANDSHAKE: Failed to send success response to %s: %v", peerID, err)
		s.Reset()
		return
	}
}

func (n *Node) initiateNodeHandshake(p peer.ID) {
	var hwInfo model.HardwareInfo
	for i := 0; i < 5; i++ {
		hwInfo = n.hardwareMonitor.GetHardwareInfo()
		if hwInfo.CgroupLimits.CPUQuotaCores > 0 {
			break
		}
		log.Printf("HANDSHAKE: Waiting for valid hardware info to send to %s...", p)
		time.Sleep(1 * time.Second)
	}

	if hwInfo.CgroupLimits.CPUQuotaCores <= 0 {
		log.Printf("HANDSHAKE: ERROR: Could not get valid local hardware info. Aborting handshake with %s.", p)
		return
	}

	log.Printf("HANDSHAKE: Initiating node-hello with peer %s", p)
	s, err := n.Host.NewStream(n.ctx, p, NodeHelloProtocolID)
	if err != nil {
		log.Printf("HANDSHAKE: Failed to open node-hello stream to %s: %v", p, err)
		return
	}
	defer s.Close()

	hwInfo.Role = n.Role
	payload := NodeHelloPayload{
		HardwareInfo: hwInfo,
		OwnerAddress: n.OwnerAddress,
		Version:      GrapthwayVersion,
		NetworkID:    GrapthwayNetworkID,
	}

	if err := json.NewEncoder(s).Encode(&payload); err != nil {
		log.Printf("HANDSHAKE: Failed to send hello payload to %s: %v", p, err)
		s.Reset()
		return
	}

	var response HandshakeResponse
	if err := json.NewDecoder(s).Decode(&response); err != nil {
		log.Printf("HANDSHAKE: Did not receive a clear response from peer %s. Assuming rejection. Error: %v", p, err)
		n.Host.Network().ClosePeer(p)
		return
	}

	if !response.Success {
		log.Printf("HANDSHAKE: Connection rejected by peer %s. Reason: %s", p, response.Reason)
		n.Host.Network().ClosePeer(p)
		return
	}

	log.Printf("HANDSHAKE: Handshake with %s successful. Proceeding to full sync.", p)
	n.syncMutex.Lock()
	if n.isSynced.Load() {
		n.syncMutex.Unlock()
		log.Printf("SYNC: Node is already synced. Skipping sync from peer %s.", p)
		return
	}
	n.syncMutex.Unlock()

	n.requestFullSyncFromPeer(p)
}

func (n *Node) handlePeerExchangeStream(s network.Stream) {
	defer s.Close()
	remotePeer := s.Conn().RemotePeer()
	log.Printf("PEER EXCHANGE: Received peer request from %s", remotePeer)

	var ourPeers []string
	const maxPeersToShare = 50

	for _, addr := range n.Host.Addrs() {
		ourPeers = append(ourPeers, fmt.Sprintf("%s/p2p/%s", addr.String(), n.Host.ID().String()))
	}

	connectedPeers := n.Host.Network().Peers()
	count := 0
	for _, peerID := range connectedPeers {
		if peerID == n.Host.ID() || peerID == remotePeer {
			continue
		}

		addrs := n.Host.Peerstore().Addrs(peerID)
		if len(addrs) == 0 {
			continue
		}

		for _, addr := range addrs {
			if count >= maxPeersToShare {
				break
			}
			ourPeers = append(ourPeers, fmt.Sprintf("%s/p2p/%s", addr.String(), peerID.String()))
			count++
		}
		if count >= maxPeersToShare {
			break
		}
	}

	if err := json.NewEncoder(s).Encode(ourPeers); err != nil {
		log.Printf("PEER EXCHANGE: Failed to send our peer list to %s: %v", remotePeer, err)
	} else {
		log.Printf("PEER EXCHANGE: Sent %d peer addresses to %s", len(ourPeers), remotePeer)
	}
}

func (n *Node) requestPeersFrom(p peer.ID) {
	log.Printf("PEER EXCHANGE: Requesting peer list from %s", p)
	s, err := n.Host.NewStream(n.ctx, p, PeerExchangeProtocolID)
	if err != nil {
		log.Printf("PEER EXCHANGE: Failed to open stream to %s: %v", p, err)
		return
	}
	defer s.Close()

	var peerList []string
	if err := json.NewDecoder(s).Decode(&peerList); err != nil {
		return
	}

	log.Printf("PEER EXCHANGE: Received %d peer addresses from %s", len(peerList), p)
	for _, addrStr := range peerList {
		addr, err := multiaddr.NewMultiaddr(addrStr)
		if err != nil {
			continue
		}
		pi, err := peer.AddrInfoFromP2pAddr(addr)
		if err != nil {
			continue
		}
		if pi.ID == n.Host.ID() {
			continue
		}
		n.Host.Peerstore().AddAddrs(pi.ID, pi.Addrs, time.Hour)
	}
}

func (n *Node) listenForConnectionEvents() {
	sub, err := n.Host.EventBus().Subscribe(new(event.EvtPeerConnectednessChanged))
	if err != nil {
		log.Printf("P2P EVENTBUS: Failed to subscribe to peer connectedness events: %v", err)
		return
	}
	defer sub.Close()

	for {
		select {
		case <-n.ctx.Done():
			return
		case e := <-sub.Out():
			evt := e.(event.EvtPeerConnectednessChanged)
			switch evt.Connectedness {
			case network.Connected:
				log.Printf("P2P EVENTBUS: Successfully established connection to peer %s.", evt.Peer)
				n.Host.Peerstore().AddAddrs(evt.Peer, n.Host.Peerstore().Addrs(evt.Peer), time.Hour)

				if !n.isSynced.Load() {
					log.Printf("P2P EVENTBUS: Node not synced, initiating handshake with new peer %s.", evt.Peer)
					go n.initiateNodeHandshake(evt.Peer)
				}
				if n.stakingProvider != nil {
					go n.stakingProvider.HandlePeerConnectionEvent(evt.Peer)
				}
				go n.requestPeersFrom(evt.Peer)
			case network.NotConnected:
				log.Printf("P2P EVENTBUS: Peer %s disconnected.", evt.Peer)
				if n.networkHardwareMonitor != nil {
					n.networkHardwareMonitor.RemovePeer(evt.Peer.String())
				}
				if n.stakingProvider != nil {
					log.Printf("P2P EVENTBUS: Peer %s disconnected, triggering validator set and leader re-evaluation.", evt.Peer)
					go n.stakingProvider.HandlePeerConnectionEvent(evt.Peer)
				}
			case network.CanConnect:
				log.Printf("P2P EVENTBUS: Now able to connect to peer %s.", evt.Peer)
			case network.CannotConnect:
				log.Printf("P2P EVENTBUS: Cannot connect to peer %s.", evt.Peer)
			}
		}
	}
}

func (n *Node) ConnectToPeerByID(p peer.ID) error {
	if n.Host.Network().Connectedness(p) == network.Connected {
		return nil
	}

	log.Printf("P2P: Proactively attempting to connect to validator %s", p)

	var pi peer.AddrInfo
	var err error

	for i := 0; i < 3; i++ {
		findCtx, findCancel := context.WithTimeout(n.ctx, 10*time.Second)
		pi, err = n.DHT.FindPeer(findCtx, p)
		findCancel()

		if err != nil {
			pi = n.Host.Peerstore().PeerInfo(p)
			if len(pi.Addrs) == 0 {
				err = fmt.Errorf("could not find address for validator %s in peerstore or DHT: %w", p, err)
				time.Sleep(1 * time.Second)
				continue
			}
		}
		err = nil
		break
	}

	if err != nil {
		return err
	}

	connectCtx, connectCancel := context.WithTimeout(n.ctx, 15*time.Second)
	defer connectCancel()
	if err := n.Host.Connect(connectCtx, pi); err != nil {
		return fmt.Errorf("failed proactive connect to validator %s: %w", p, err)
	}
	log.Printf("P2P: Proactively connected to validator %s", p)
	return nil
}

func (n *Node) maintainNetworkConnectivity() {
	lowPeerTicker := time.NewTicker(10 * time.Second)
	defer lowPeerTicker.Stop()
	validatorMeshTicker := time.NewTicker(30 * time.Second)
	defer validatorMeshTicker.Stop()

	for {
		select {
		case <-n.ctx.Done():
			return
		case <-lowPeerTicker.C:
			if n.isSynced.Load() == false {
				log.Println("NETWORK WATCHDOG: Node not synced yet, skipping peer maintenance.")
				continue
			}
			peerCount := len(n.Host.Network().Peers())
			log.Printf("NETWORK WATCHDOG: Running check. Current connections: %d. Low water mark: %d.", peerCount, peerLowWaterMark)
			if peerCount < peerLowWaterMark {
				log.Printf("NETWORK WATCHDOG: Low peer count (%d < %d). Re-initiating bootstrap to heal.", peerCount, peerLowWaterMark)

				connectedPeers := make(map[peer.ID]bool)
				for _, p := range n.Host.Network().Peers() {
					connectedPeers[p] = true
				}
				log.Printf("NETWORK WATCHDOG: Found %d currently connected peers.", len(connectedPeers))

				peerAddrSet := make(map[string]struct{})

				for _, addrStr := range n.initialBootstrapPeers {
					addr, err := multiaddr.NewMultiaddr(addrStr)
					if err != nil {
						continue
					}
					pi, err := peer.AddrInfoFromP2pAddr(addr)
					if err != nil {
						continue
					}
					if connectedPeers[pi.ID] {
						continue
					}
					if n.isPeerDenied(pi.ID) {
						continue
					}
					peerAddrSet[addrStr] = struct{}{}
				}

				for _, p := range n.Host.Peerstore().Peers() {
					if p == n.Host.ID() || connectedPeers[p] {
						continue
					}
					if n.isPeerDenied(p) {
						continue
					}
					addrs := n.Host.Peerstore().Addrs(p)
					for _, addr := range addrs {
						fullAddr := fmt.Sprintf("%s/p2p/%s", addr.String(), p.String())
						peerAddrSet[fullAddr] = struct{}{}
					}
				}

				dynamicPeerList := make([]string, 0, len(peerAddrSet))
				for addr := range peerAddrSet {
					dynamicPeerList = append(dynamicPeerList, addr)
				}

				if len(dynamicPeerList) > 0 {
					log.Printf("NETWORK WATCHDOG: Attempting to heal by connecting to %d non-connected peers.", len(dynamicPeerList))
					go n.bootstrapConnect(dynamicPeerList)
				} else {
					log.Println("NETWORK WATCHDOG: No non-connected peers available to connect to. Will try DHT bootstrap again.")
					if err := n.DHT.Bootstrap(n.ctx); err != nil {
						log.Printf("NETWORK WATCHDOG: DHT bootstrap during healing failed: %v", err)
					}
				}
			}
		case <-validatorMeshTicker.C:
			if n.stakingProvider == nil || !n.isSynced.Load() {
				continue
			}
			validators := n.stakingProvider.GetValidatorSet()
			if len(validators) == 0 {
				continue
			}
			log.Printf("VALIDATOR MESH: Verifying connectivity to %d validators.", len(validators))
			for _, v := range validators {
				validatorID := v.PeerID
				if validatorID == n.Host.ID() {
					continue
				}
				if n.Host.Network().Connectedness(validatorID) != network.Connected {
					log.Printf("VALIDATOR MESH: Not connected to validator %s. Attempting to connect.", validatorID)
					go n.ConnectToPeerByID(validatorID)
				}
			}
		}
	}
}

func (n *Node) bootstrapConnect(peers []string) {
	if len(peers) == 0 {
		log.Println("P2P: No bootstrap peers provided. Attempting DHT bootstrap.")
		if err := n.DHT.Bootstrap(n.ctx); err != nil {
			log.Printf("DHT bootstrap failed: %v", err)
		}
		return
	}

	log.Printf("P2P: Attempting to connect to %d candidate peers...", len(peers))

	var wg sync.WaitGroup
	for _, addrStr := range peers {
		if addrStr == "" {
			continue
		}

		addr, err := multiaddr.NewMultiaddr(addrStr)
		if err != nil {
			log.Printf("Error parsing bootstrap multiaddr '%s': %v", addrStr, err)
			continue
		}
		peerInfo, err := peer.AddrInfoFromP2pAddr(addr)
		if err != nil {
			log.Printf("Error getting peer info from multiaddr '%s': %v", addrStr, err)
			continue
		}

		if peerInfo.ID == n.Host.ID() {
			continue
		}

		if n.isPeerDenied(peerInfo.ID) {
			log.Printf("P2P: Skipping connection attempt to denied peer %s", peerInfo.ID)
			continue
		}

		if n.Host.Network().Connectedness(peerInfo.ID) == network.Connected {
			continue
		}

		wg.Add(1)
		go func(pi peer.AddrInfo) {
			defer wg.Done()
			log.Printf("P2P BOOTSTRAP: Attempting to connect to %s", pi.ID)
			ctx, cancel := context.WithTimeout(n.ctx, 15*time.Second)
			defer cancel()
			if err := n.Host.Connect(ctx, pi); err != nil {
				if !strings.Contains(err.Error(), "already connected") {
					log.Printf("Failed to connect to peer %s: %v", pi.ID, err)
				}
			} else {
				log.Printf("Successfully connected to bootstrap peer: %s", pi.ID.String())
			}
		}(*peerInfo)
	}
	wg.Wait()

	log.Println("P2P: Connection attempts complete. Bootstrapping DHT for wider network discovery...")
	if err := n.DHT.Bootstrap(n.ctx); err != nil {
		log.Printf("DHT bootstrap failed: %v", err)
	} else {
		log.Println("P2P: DHT bootstrap process completed.")
	}
}

func (n *Node) IsPeerConnected(p peer.ID) bool {
	return n.Host.Network().Connectedness(p) == network.Connected
}

func (n *Node) Gossip(topic string, message []byte) error {
	if !n.isSynced.Load() {
		return fmt.Errorf("node is not synced, cannot gossip")
	}
	return n.PubSub.Publish(topic, message)
}

func (n *Node) Subscribe(topicName string, waitForSync bool) (<-chan *pubsub.Message, error) {
	n.subMutex.Lock()
	defer n.subMutex.Unlock()

	if _, exists := n.subscriptions[topicName]; exists {
		log.Printf("WARN: Attempted to subscribe to already-joined topic %s.", topicName)
		return nil, fmt.Errorf("topic %s already joined", topicName)
	}

	topic, err := n.PubSub.Join(topicName)
	if err != nil {
		return nil, fmt.Errorf("failed to join topic %s: %w", topicName, err)
	}

	sub, err := topic.Subscribe()
	if err != nil {
		topic.Close()
		return nil, fmt.Errorf("failed to subscribe to topic %s: %w", topicName, err)
	}

	n.subscriptions[topicName] = sub
	msgChan := make(chan *pubsub.Message, 256)

	go func() {
		defer close(msgChan)
		defer sub.Cancel()
		defer topic.Close()

		if waitForSync {
			<-n.syncComplete
			log.Printf("PUBSUB: Node synced. Now processing messages for stateful topic %s", topicName)
		} else {
			log.Printf("PUBSUB: Immediately processing messages for stateless topic %s", topicName)
		}

		for {
			select {
			case <-n.ctx.Done():
				return
			default:
				msg, err := sub.Next(n.ctx)
				if err != nil {
					if n.ctx.Err() != nil {
						return
					}
					log.Printf("Error reading from subscription to %s: %v", topicName, err)
					continue
				}
				if msg.ReceivedFrom == n.Host.ID() {
					continue
				}
				msgChan <- msg
			}
		}
	}()

	return msgChan, nil
}

func (n *Node) CanBroadcastState() bool {
	const broadcastInterval = int64(time.Second)
	now := time.Now().UnixNano()
	last := atomic.LoadInt64(&n.lastStateBroadcast)
	if now-last < broadcastInterval {
		return false
	}
	return atomic.CompareAndSwapInt64(&n.lastStateBroadcast, last, now)
}

func (n *Node) GetPeerList() []peer.ID {
	return n.Host.Network().Peers()
}

func (n *Node) Close() error {
	n.subMutex.Lock()
	for name, sub := range n.subscriptions {
		sub.Cancel()
		delete(n.subscriptions, name)
	}
	n.subMutex.Unlock()
	return n.Host.Close()
}

func (n *Node) subscribeAndProcessAcks(ctx context.Context) {
	topic, err := n.PubSub.Join(LedgerMempoolAckTopic)
	if err != nil {
		log.Printf("P2P: FATAL: could not join mempool ACK topic: %v", err)
		return
	}
	sub, err := topic.Subscribe()
	if err != nil {
		log.Printf("P2P: FATAL: could not subscribe to mempool ACK topic: %v", err)
		return
	}
	defer sub.Cancel()
	defer topic.Close()

	log.Printf("PUBSUB: Now processing messages for topic %s", LedgerMempoolAckTopic)

	for {
		msg, err := sub.Next(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("P2P: Error reading from mempool ACK subscription: %v", err)
			continue
		}
		if msg.ReceivedFrom == n.Host.ID() {
			continue
		}
		n.handleMempoolAck(msg)
	}
}

func (n *Node) handleMempoolAck(msg *pubsub.Message) {
	var ackTxID string
	if err := json.Unmarshal(msg.Data, &ackTxID); err != nil {
		return
	}

	n.ackChannelsMutex.Lock()
	ch, exists := n.ackChannels[ackTxID]
	n.ackChannelsMutex.Unlock()

	if exists {
		select {
		case ch <- msg.ReceivedFrom:
		default:
		}
	}
}

func (n *Node) BroadcastMempoolAck(txID string) error {
	ackBytes, err := json.Marshal(txID)
	if err != nil {
		log.Printf("P2P: Failed to marshal mempool ACK for %s: %v", txID, err)
		return err
	}
	return n.Gossip(LedgerMempoolAckTopic, ackBytes)
}

func (n *Node) WaitForPeerAcks(ctx context.Context, txID string, requiredAcks int) error {
	if requiredAcks <= 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	ackChan := make(chan peer.ID, len(n.GetPeerList()))

	n.ackChannelsMutex.Lock()
	n.ackChannels[txID] = ackChan
	n.ackChannelsMutex.Unlock()

	defer func() {
		n.ackChannelsMutex.Lock()
		delete(n.ackChannels, txID)
		n.ackChannelsMutex.Unlock()
		close(ackChan)
	}()

	receivedAcks := make(map[peer.ID]struct{})
	log.Printf("QUORUM: Waiting for %d peer ACK(s) for tx %s...", requiredAcks, txID)

	for {
		select {
		case peerID := <-ackChan:
			if _, exists := receivedAcks[peerID]; !exists {
				receivedAcks[peerID] = struct{}{}
				log.Printf("QUORUM: Received ACK for %s from %s (%d/%d)", txID, peerID, len(receivedAcks), requiredAcks)
				if len(receivedAcks) >= requiredAcks {
					return nil
				}
			}
		case <-ctx.Done():
			return fmt.Errorf("context canceled while waiting for quorum for tx %s (received %d/%d ACKs)", txID, len(receivedAcks), requiredAcks)
		}
	}
}
