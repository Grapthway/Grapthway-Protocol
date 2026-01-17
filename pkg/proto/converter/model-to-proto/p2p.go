package modeltoproto

import (
	"grapthway/pkg/model"
	pb "grapthway/pkg/proto"
)

func ModelToProtoPreBlockAck(m *model.PreBlockAck) *pb.PreBlockAck {
	if m == nil {
		return nil
	}
	return &pb.PreBlockAck{
		ProposalId:   m.ProposalID,
		ValidatorId:  m.ValidatorID,
		ValidatorSig: m.ValidatorSig,
	}
}

func ModelToProtoNextBlockReady(m *model.NextBlockReady) *pb.NextBlockReady {
	if m == nil {
		return nil
	}
	return &pb.NextBlockReady{
		BlockHeight: m.BlockHeight,
		ValidatorId: m.ValidatorID,
	}
}
