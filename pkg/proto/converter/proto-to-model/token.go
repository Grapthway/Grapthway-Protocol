package prototomodel

import (
	"grapthway/pkg/ledger/types"
	pb "grapthway/pkg/proto"
)

func protoToModelBurnConfig(p *pb.BurnConfig) *types.BurnConfig {
	if p == nil {
		return &types.BurnConfig{}
	}
	return &types.BurnConfig{
		Enabled:       p.Enabled,
		BurnRatePerTx: p.BurnRatePerTx,
		ManualBurn:    p.ManualBurn,
	}
}

func protoToModelMintConfig(p *pb.MintConfig) *types.MintConfig {
	if p == nil {
		return &types.MintConfig{}
	}
	return &types.MintConfig{
		Enabled:       p.Enabled,
		MintRatePerTx: p.MintRatePerTx,
		ManualMint:    p.ManualMint,
	}
}

func ProtoToModelTokenState(p *pb.TokenState) *types.Token {
	if p == nil {
		return nil
	}
	return &types.Token{
		Address:       p.Address,
		Name:          p.Name,
		Ticker:        p.Ticker,
		Decimals:      uint8(p.Decimals),
		TotalSupply:   p.TotalSupply,
		Creator:       p.Creator,
		CreatedAt:     p.CreatedAt.AsTime(),
		ConfigLocked:  p.ConfigLocked,
		InitialSupply: p.InitialSupply,
		TokenType:     types.TokenType(p.TokenType),
		BurnConfig:    *protoToModelBurnConfig(p.BurnConfig),
		MintConfig:    *protoToModelMintConfig(p.MintConfig),
		Metadata:      p.Metadata,
	}
}
