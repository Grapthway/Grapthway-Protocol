package prototomodel

import (
	"grapthway/pkg/ledger/types"
	pb "grapthway/pkg/proto"

	"github.com/libp2p/go-libp2p/core/peer"
)

func protoToModelValidatorInfo(p *pb.ValidatorInfo) *types.ValidatorInfo {
	if p == nil {
		return nil
	}
	peerID, _ := peer.Decode(p.PeerId)
	return &types.ValidatorInfo{
		PeerID:     peerID,
		TotalStake: p.TotalStake,
	}
}

func protoToModelTokenMetadata(p *pb.TokenMetadata) *types.TokenMetadata {
	if p == nil {
		return nil
	}
	burnConfig := protoToModelBurnConfig(p.BurnConfig)
	if burnConfig == nil {
		burnConfig = &types.BurnConfig{}
	}
	mintConfig := protoToModelMintConfig(p.MintConfig)
	if mintConfig == nil {
		mintConfig = &types.MintConfig{}
	}
	return &types.TokenMetadata{
		Name:          p.Name,
		Ticker:        p.Ticker,
		Decimals:      uint8(p.Decimals),
		InitialSupply: p.InitialSupply,
		TokenType:     p.TokenType,
		BurnConfig:    *burnConfig,
		MintConfig:    *mintConfig,
		Metadata:      p.Metadata,
	}
}

// protoToModelTransaction now deserializes token-related fields.
func ProtoToModelTransaction(p *pb.Transaction) *types.Transaction {
	if p == nil {
		return nil
	}
	m := &types.Transaction{
		ID:             p.Id,
		Type:           types.TransactionType(pb.TransactionType_name[int32(p.Type)]),
		From:           p.From,
		To:             p.To,
		Amount:         p.Amount,
		Nonce:          p.Nonce,
		Timestamp:      p.Timestamp.AsTime(),
		CreatedAt:      p.CreatedAt.AsTime(),
		TargetNodeID:   p.TargetNodeId,
		Spender:        p.Spender,
		AllowanceLimit: p.AllowanceLimit,
		TokenAddress:   p.TokenAddress,
		TokenMetadata:  protoToModelTokenMetadata(p.TokenMetadata),
	}
	if p.BatchPayload != nil {
		m.BatchPayload = &types.BatchAggregationPayload{
			Debits:          p.BatchPayload.Debits,
			Rewards:         p.BatchPayload.Rewards,
			AggregatedTxIDs: p.BatchPayload.AggregatedTxIds,
		}
	}
	return m
}

func ProtoToModelBlock(p *pb.Block) *types.Block {
	if p == nil {
		return nil
	}
	txs := make([]types.Transaction, len(p.Transactions))
	for i, tx := range p.Transactions {
		txs[i] = *ProtoToModelTransaction(tx)
	}

	nextValidators := make([]types.ValidatorInfo, len(p.NextValidators))
	for i, v := range p.NextValidators {
		val := protoToModelValidatorInfo(v)
		if val != nil {
			nextValidators[i] = *val
		}
	}

	return &types.Block{
		ProposerID:     p.ProposerId,
		Transactions:   txs,
		Timestamp:      p.Timestamp.AsTime(),
		Hash:           p.Hash,
		Height:         p.Height,
		PrevHash:       p.PrevHash,
		NextValidators: nextValidators,
	}
}

func ProtoToModelAccountState(p *pb.AccountState) *types.AccountState {
	if p == nil {
		return nil
	}
	return &types.AccountState{
		Address:      p.Address,
		Balance:      p.Balance,
		Nonce:        p.Nonce,
		PendingNonce: p.PendingNonce,
		PendingSpend: p.PendingSpend,
		StakeInfo:    protoToModelStakeInfo(p.StakeInfo),
		Allowances:   p.Allowances,
	}
}

func protoToModelStakeAllocation(p *pb.StakeAllocation) types.StakeAllocation {
	if p == nil {
		return types.StakeAllocation{}
	}
	return types.StakeAllocation{
		NodePeerID: p.NodePeerId,
		Amount:     p.Amount,
		LockedAt:   p.LockedAt.AsTime(),
	}
}

func protoToModelUnbondingStake(p *pb.UnbondingStake) types.UnbondingStake {
	if p == nil {
		return types.UnbondingStake{}
	}
	return types.UnbondingStake{
		Amount:     p.Amount,
		UnlockTime: p.UnlockTime.AsTime(),
	}
}

func protoToModelStakeInfo(p *pb.StakeInfo) *types.StakeInfo {
	if p == nil {
		return nil
	}
	allocs := make([]types.StakeAllocation, len(p.Allocations))
	for i, a := range p.Allocations {
		allocs[i] = protoToModelStakeAllocation(a)
	}
	unbonds := make([]types.UnbondingStake, len(p.Unbonding))
	for i, u := range p.Unbonding {
		unbonds[i] = protoToModelUnbondingStake(u)
	}
	return &types.StakeInfo{
		TotalStake:  p.TotalStake,
		Allocations: allocs,
		Unbonding:   unbonds,
	}
}
