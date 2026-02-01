package modeltoproto

import (
	"grapthway/pkg/ledger/types"
	pb "grapthway/pkg/proto"

	"google.golang.org/protobuf/types/known/timestamppb"
)

func ModelToProtoTransaction(m *types.Transaction) *pb.Transaction {
	if m == nil {
		return nil
	}

	txType, _ := pb.TransactionType_value[string(m.Type)]
	p := &pb.Transaction{
		Id:             m.ID,
		Type:           pb.TransactionType(txType),
		From:           m.From,
		To:             m.To,
		Amount:         m.Amount,
		Nonce:          m.Nonce,
		Timestamp:      timestamppb.New(m.Timestamp),
		CreatedAt:      timestamppb.New(m.CreatedAt),
		TargetNodeId:   m.TargetNodeID,
		Spender:        m.Spender,
		AllowanceLimit: m.AllowanceLimit,
		TokenAddress:   m.TokenAddress,
	}

	if m.BatchPayload != nil {
		p.BatchPayload = &pb.BatchAggregationPayload{
			Debits:          m.BatchPayload.Debits,
			Rewards:         m.BatchPayload.Rewards,
			AggregatedTxIds: m.BatchPayload.AggregatedTxIDs,
		}
	}

	if m.TokenMetadata != nil {
		p.TokenMetadata = &pb.TokenMetadata{
			Name:          m.TokenMetadata.Name,
			Ticker:        m.TokenMetadata.Ticker,
			Decimals:      uint32(m.TokenMetadata.Decimals),
			InitialSupply: m.TokenMetadata.InitialSupply,
			TokenType:     m.TokenMetadata.TokenType,
			Metadata:      m.TokenMetadata.Metadata,
		}

		// Create BurnConfig with ALL fields
		p.TokenMetadata.BurnConfig = &pb.BurnConfig{
			Enabled:       m.TokenMetadata.BurnConfig.Enabled,
			BurnRatePerTx: m.TokenMetadata.BurnConfig.BurnRatePerTx,
			ManualBurn:    m.TokenMetadata.BurnConfig.ManualBurn,
		}

		// Create MintConfig with ALL fields
		p.TokenMetadata.MintConfig = &pb.MintConfig{
			Enabled:       m.TokenMetadata.MintConfig.Enabled,
			MintRatePerTx: m.TokenMetadata.MintConfig.MintRatePerTx,
			ManualMint:    m.TokenMetadata.MintConfig.ManualMint,
		}
	}

	return p
}

func modelToProtoValidatorInfo(m *types.ValidatorInfo) *pb.ValidatorInfo {
	if m == nil {
		return nil
	}
	return &pb.ValidatorInfo{
		PeerId:     m.PeerID.String(),
		TotalStake: m.TotalStake,
	}
}

func ModelToProtoBlock(m *types.Block) *pb.Block {
	if m == nil {
		return nil
	}
	txs := make([]*pb.Transaction, len(m.Transactions))
	for i, tx := range m.Transactions {
		txs[i] = ModelToProtoTransaction(&tx)
	}
	pbValidators := make([]*pb.ValidatorInfo, len(m.NextValidators))
	for i, v := range m.NextValidators {
		pbValidators[i] = modelToProtoValidatorInfo(&v)
	}
	return &pb.Block{
		ProposalId:     m.ProposalID,
		ProposerId:     m.ProposerID,
		Transactions:   txs,
		Timestamp:      timestamppb.New(m.Timestamp),
		Hash:           m.Hash,
		Height:         m.Height,
		PrevHash:       m.PrevHash,
		NextValidators: pbValidators,
	}
}

func ModelToProtoAccountState(m *types.AccountState) *pb.AccountState {
	if m == nil {
		return nil
	}
	return &pb.AccountState{
		Address:      m.Address,
		Balance:      m.Balance,
		Nonce:        m.Nonce,
		PendingNonce: m.PendingNonce,
		PendingSpend: m.PendingSpend,
		StakeInfo:    modelToProtoStakeInfo(m.StakeInfo),
		Allowances:   m.Allowances,
	}
}

func modelToProtoStakeAllocation(m *types.StakeAllocation) *pb.StakeAllocation {
	if m == nil {
		return nil
	}
	return &pb.StakeAllocation{
		NodePeerId: m.NodePeerID,
		Amount:     m.Amount,
		LockedAt:   timestamppb.New(m.LockedAt),
	}
}

func modelToProtoUnbondingStake(m *types.UnbondingStake) *pb.UnbondingStake {
	if m == nil {
		return nil
	}
	return &pb.UnbondingStake{
		Amount:     m.Amount,
		UnlockTime: timestamppb.New(m.UnlockTime),
	}
}

func modelToProtoStakeInfo(m *types.StakeInfo) *pb.StakeInfo {
	if m == nil {
		return nil
	}
	allocs := make([]*pb.StakeAllocation, len(m.Allocations))
	for i, a := range m.Allocations {
		allocs[i] = modelToProtoStakeAllocation(&a)
	}
	unbonds := make([]*pb.UnbondingStake, len(m.Unbonding))
	for i, u := range m.Unbonding {
		unbonds[i] = modelToProtoUnbondingStake(&u)
	}
	return &pb.StakeInfo{
		TotalStake:  m.TotalStake,
		Allocations: allocs,
		Unbonding:   unbonds,
	}
}
