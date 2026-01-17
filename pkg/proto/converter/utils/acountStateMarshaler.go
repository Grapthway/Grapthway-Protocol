package utils

import (
	"grapthway/pkg/ledger/types"
	pb "grapthway/pkg/proto"
	modeltoproto "grapthway/pkg/proto/converter/model-to-proto"
	prototomodel "grapthway/pkg/proto/converter/proto-to-model"

	"github.com/gogo/protobuf/proto"
)

// In utils package
func MarshalAccountState(as *types.AccountState) ([]byte, error) {
	pbState := modeltoproto.ModelToProtoAccountState(as)
	return proto.Marshal(pbState)
}

func UnmarshalAccountState(data []byte) (*types.AccountState, error) {
	var pbState pb.AccountState
	if err := proto.Unmarshal(data, &pbState); err != nil {
		return nil, err
	}
	return prototomodel.ProtoToModelAccountState(&pbState), nil
}
