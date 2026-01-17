package utils

import (
	"fmt"
	"grapthway/pkg/ledger/types"
	"sort"
	"strings"
)

func DeterministicKey(p *types.BatchAggregationPayload) string {
	var debitKeys, rewardKeys []string
	for k := range p.Debits {
		debitKeys = append(debitKeys, k)
	}
	for k := range p.Rewards {
		rewardKeys = append(rewardKeys, k)
	}
	sort.Strings(debitKeys)
	sort.Strings(rewardKeys)

	var sb strings.Builder
	sb.WriteString("d:")
	for _, k := range debitKeys {
		sb.WriteString(fmt.Sprintf("%s-%d;", k, p.Debits[k]))
	}
	sb.WriteString("r:")
	for _, k := range rewardKeys {
		sb.WriteString(fmt.Sprintf("%s-%d;", k, p.Rewards[k]))
	}
	return sb.String()
}
