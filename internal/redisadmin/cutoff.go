package redisadmin

// GroupState is what the cleaner needs to know about a consumer group.
type GroupState struct {
	Name          string
	LastDelivered ID
	OldestPending *ID // nil when the group has nothing pending
}

// SafeCutoff returns the MINID below which every entry is acknowledged by
// every group and at least as old as ageCutoff; ok=false when groups is empty.
func SafeCutoff(ageCutoff ID, groups []GroupState) (cutoff ID, ok bool) {
	if len(groups) == 0 {
		return ID{}, false
	}
	cutoff = ageCutoff
	for _, g := range groups {
		if next := g.LastDelivered.Next(); next.Less(cutoff) {
			cutoff = next
		}
		if g.OldestPending != nil && g.OldestPending.Less(cutoff) {
			cutoff = *g.OldestPending
		}
	}
	return cutoff, true
}
