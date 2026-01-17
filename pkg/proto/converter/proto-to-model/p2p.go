package prototomodel

import (
	"grapthway/pkg/model"
	pb "grapthway/pkg/proto"
)

func ProtoToModelPreBlockAck(p *pb.PreBlockAck) *model.PreBlockAck {
	if p == nil {
		return nil
	}
	return &model.PreBlockAck{
		ProposalID:   p.ProposalId,
		ValidatorID:  p.ValidatorId,
		ValidatorSig: p.ValidatorSig,
	}
}

func ProtoToModelNextBlockReady(p *pb.NextBlockReady) *model.NextBlockReady {
	if p == nil {
		return nil
	}
	return &model.NextBlockReady{
		BlockHeight: p.BlockHeight,
		ValidatorID: p.ValidatorId,
	}
}
