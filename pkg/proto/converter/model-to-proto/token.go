package modeltoproto

import (
	"grapthway/pkg/ledger/types"
	pb "grapthway/pkg/proto"

	"google.golang.org/protobuf/types/known/timestamppb"
)

func modelToProtoBurnConfig(m *types.BurnConfig) *pb.BurnConfig {
	if m == nil {
		return nil
	}
	return &pb.BurnConfig{
		Enabled:       m.Enabled,
		BurnRatePerTx: m.BurnRatePerTx,
		ManualBurn:    m.ManualBurn,
	}
}

func modelToProtoMintConfig(m *types.MintConfig) *pb.MintConfig {
	if m == nil {
		return nil
	}
	return &pb.MintConfig{
		Enabled:       m.Enabled,
		MintRatePerTx: m.MintRatePerTx,
		ManualMint:    m.ManualMint,
	}
}

func ModelToProtoTokenState(m *types.Token) *pb.TokenState {
	if m == nil {
		return nil
	}
	return &pb.TokenState{
		Address:       m.Address,
		Name:          m.Name,
		Ticker:        m.Ticker,
		Decimals:      uint32(m.Decimals),
		TotalSupply:   m.TotalSupply,
		Creator:       m.Creator,
		CreatedAt:     timestamppb.New(m.CreatedAt),
		ConfigLocked:  m.ConfigLocked,
		InitialSupply: m.InitialSupply,
		TokenType:     string(m.TokenType),
		BurnConfig:    modelToProtoBurnConfig(&m.BurnConfig),
		MintConfig:    modelToProtoMintConfig(&m.MintConfig),
		Metadata:      m.Metadata,
	}
}
