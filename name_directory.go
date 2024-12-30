package grinta

import (
	"errors"
	"log/slog"
	"sync"
	"time"

	grintav1alpha1 "github.com/raskyld/grinta/gen/grinta/v1alpha1"
)

// nameDirectory is an eventually consistent and distributed radix `Tree`
// containing information about where each endpoint lives.
type nameDirectory struct {
	d       *Tree[*nameRecord]
	lk      sync.RWMutex
	closeCh chan struct{}

	// a local monotonic clock to order our local changes.
	clock uint64

	activeConflicts map[string]*conflictRecord
	conflictTimeout time.Duration
	conflictTicker  *time.Ticker

	logger        *slog.Logger
	gossip        nameGossip
	localNodeName string
}

type nameRecord struct {
	active          *grintav1alpha1.NameClaim
	claimantHistory map[string]*grintav1alpha1.NameClaim

	// lc has a "level" which is the last claiment's name
	// the "edge" is when we last saw a change between the "level"
	// (i.e., a different claimant).
	// this knowledge is useful to see if there is recent contention
	// about a name.
	lcLevel string
	lcEdge  time.Time
}

type nameGossip interface {
	// askConsensus to all peers, they will dump their most recent claim
	// for the specified endpoint name, so we can follow majority.
	//
	// If a split-brain occurs (no clear majority), default to give ownership
	// to lexico-smallest host name.
	askConsensus(epName string) (uint64, error)

	// confirmClaim ask a specific peer to confirm its ownership of a name.
	//
	// A revision can be provided so the peer only need to send its claim back
	// if a more recent revision is available.
	confirmClaim(peer, epName string, lastRev uint64) (uint64, error)

	getConsensusResult(id uint64) (*consensusResult, error)
	getClaimResult(id uint64) (*grintav1alpha1.NameOwnershipResponse, error)

	cancel(id uint64)
}

type consensusResult struct {
	activePerHost map[string]*grintav1alpha1.NameClaim
	answers       int
	clusterSize   int
}

type conflictRecord struct {
	deadline time.Time
	id       uint64
}

type voteKey struct {
	host string
	mode grintav1alpha1.NameClaimMode
}

type voteVal struct {
	count      int
	mostRecent *grintav1alpha1.NameClaim
}

func newNameDir(logger *slog.Logger, gossip nameGossip, localNodeName string) *nameDirectory {
	dir := &nameDirectory{
		d:               NewTree[*nameRecord](),
		activeConflicts: make(map[string]*conflictRecord),
		logger:          logger,
		conflictTimeout: 10 * time.Second,
		conflictTicker:  time.NewTicker(1 * time.Second),
		closeCh:         make(chan struct{}, 1),
		gossip:          gossip,
		localNodeName:   localNodeName,
	}

	go dir.handleConflicts()

	return dir
}

func newTestableNameDir(logger *slog.Logger, gossip nameGossip, localNodeName string) *nameDirectory {
	dir := &nameDirectory{
		d:               NewTree[*nameRecord](),
		activeConflicts: make(map[string]*conflictRecord),
		logger:          logger,
		conflictTimeout: 3 * time.Second,
		conflictTicker:  time.NewTicker(200 * time.Millisecond),
		closeCh:         make(chan struct{}, 1),
		gossip:          gossip,
		localNodeName:   localNodeName,
	}

	go dir.handleConflicts()

	return dir
}

func (dir *nameDirectory) getNamesWithConflict() (conflicts []string) {
	dir.lk.RLock()
	defer dir.lk.RUnlock()
	for name := range dir.activeConflicts {
		conflicts = append(conflicts, name)
	}
	return
}

func (dir *nameDirectory) resolve(name string) (string, error) {
	dir.lk.RLock()
	defer dir.lk.RUnlock()
	currentNode, has := dir.d.Get(name)
	if !has {
		return "", ErrNameResolution
	}

	if currentNode.active.GetMode() == grintav1alpha1.NameClaimMode_NAME_CLAIM_MODE_CLAIM {
		return currentNode.active.GetNodeName(), nil
	}

	return "", ErrNameResolution
}

func (dir *nameDirectory) scan(prefix string) (found []string, err error) {
	dir.lk.RLock()
	defer dir.lk.RUnlock()
	for name, currentNode := range dir.d.WalkPrefix(prefix) {
		if currentNode.active.GetMode() == grintav1alpha1.NameClaimMode_NAME_CLAIM_MODE_CLAIM {
			found = append(found, name)
		}
	}

	if len(found) == 0 {
		err = ErrNameResolution
	}
	return
}

func (dir *nameDirectory) record(claim *grintav1alpha1.NameClaim, synchronous bool) error {
	if !claim.HasEndpointName() || !claim.HasNodeName() || (!claim.HasRev() && !synchronous) || !claim.HasMode() {
		return ErrFabricInvalidFrame
	}

	name := claim.GetEndpointName()
	claimant := claim.GetNodeName()
	rev := claim.GetRev()

	dir.lk.Lock()
	defer dir.lk.Unlock()

	now := time.Now()

	// Increment our clock and set revision in the claim.
	if synchronous {
		dir.clock = dir.clock + 1
		claim.SetRev(dir.clock)
	}

	currentNode, has := dir.d.Get(name)
	if has {
		// If the claimant is the one we have in our local state, there is
		// no problem.
		if currentNode.active.GetNodeName() == claimant {
			if rev > currentNode.active.GetRev() {
				currentNode.active = claim
			}
		} else {
			// We conservatively store history to prepare for any potential conflict.
			if currentNode.claimantHistory == nil {
				currentNode.claimantHistory = map[string]*grintav1alpha1.NameClaim{
					currentNode.active.GetNodeName(): currentNode.active,
				}
			}

			// If we try to apply a claim on a recently unclaimed name,
			// our local state can be reconciled.
			if currentNode.active.GetMode() == grintav1alpha1.NameClaimMode_NAME_CLAIM_MODE_UNCLAIM &&
				claim.GetMode() == grintav1alpha1.NameClaimMode_NAME_CLAIM_MODE_CLAIM {
				currentNode.active = claim
			} else {
				// Applying this is not possible, if we are in synchronous mode
				// we can just abort.
				if synchronous {
					return ErrNameConflict
				}

				// Otherwise, we enable conflict mode and wait in case the
				// cluster converge to resolution.
				conflict, ok := dir.activeConflicts[name]
				if ok {
					if conflict.deadline.Before(now) {
						dir.logger.Debug(
							"conflict hard resolution happens next tick",
							"endpoint_name", name,
						)
					} else {
						dir.logger.Debug(
							"time before conflict hard resolution",
							"duration", time.Until(conflict.deadline),
							"endpoint_name", name,
						)
					}
				} else {
					dir.activeConflicts[name] = &conflictRecord{
						deadline: now.Add(dir.conflictTimeout),
					}
					dir.logger.Debug("endpoint name conflict detected", "endpoint_name", name)
				}
			}
		}

		if currentNode.claimantHistory != nil {
			if currentNode.lcLevel != claimant {
				currentNode.lcLevel = claimant
				currentNode.lcEdge = now
			}
			historical, hasHistorical := currentNode.claimantHistory[claimant]
			if hasHistorical {
				if rev > historical.GetRev() {
					currentNode.claimantHistory[claimant] = claim
				}
			} else {
				currentNode.claimantHistory[claimant] = claim
			}
		}
	} else {
		dir.d.Insert(name, &nameRecord{
			active:          claim,
			claimantHistory: nil,
		})
	}
	return nil
}

func (dir *nameDirectory) handleConflicts() {
	for {
		select {
		case <-dir.conflictTicker.C:
			dir.lk.Lock()
			if len(dir.activeConflicts) == 0 {
				dir.lk.Unlock()
				continue
			}
			now := time.Now()
			for epName, conflict := range dir.activeConflicts {
				node, has := dir.d.Get(epName)
				if !has {
					if conflict.id != 0 {
						dir.gossip.cancel(conflict.id)
					}
					delete(dir.activeConflicts, epName)
					continue
				}

				var uniqueClaim *grintav1alpha1.NameClaim
				var resolved bool
				for _, claim := range node.claimantHistory {
					if claim.GetMode() == grintav1alpha1.NameClaimMode_NAME_CLAIM_MODE_CLAIM {
						if uniqueClaim != nil {
							// we still have two different claimants, conflict
							// is not resolved.
							resolved = false
							continue
						}

						resolved = true
						uniqueClaim = claim
					}
				}

				if resolved {
					dir.logger.Debug("conflict resolved", "endpoint_name", epName)
					if conflict.id != 0 {
						dir.gossip.cancel(conflict.id)
					}
					delete(dir.activeConflicts, epName)
					continue
				}

				if conflict.deadline.Before(now) {
					if conflict.id != 0 {
						result, err := dir.gossip.getConsensusResult(conflict.id)
						if err != nil {
							if errors.Is(err, ErrGossipInProgress) {
								// retry next tick
								continue
							}

							dir.logger.Error("failed consensus, retry next tick", LabelError.L(err))
							conflict.id = 0
							continue
						}

						answerRate := float64(result.answers) / float64(result.clusterSize)
						if answerRate < 0.67 {
							dir.logger.Error("failed consensus, not enough participation", "participation_rate", answerRate)
							conflict.id = 0
							continue
						}

						_, hasMyVote := result.activePerHost[dir.localNodeName]
						if !hasMyVote {
							result.activePerHost[dir.localNodeName] = node.active
						}

						votes := make(map[voteKey]*voteVal)
						var highest *voteVal
						for _, vote := range result.activePerHost {
							if vote.HasNodeName() && vote.HasMode() {
								key := voteKey{host: vote.GetNodeName(), mode: vote.GetMode()}
								curr, ok := votes[key]
								if !ok {
									curr = &voteVal{
										count:      1,
										mostRecent: vote,
									}
									votes[key] = curr
								} else {
									curr.count = curr.count + 1
									if vote.HasRev() && (!curr.mostRecent.HasRev() || vote.GetRev() > curr.mostRecent.GetRev()) {
										curr.mostRecent = vote
									}
								}

								if highest == nil || curr.count > highest.count {
									highest = curr
								}
							}
						}

						if highest == nil {
							dir.logger.Error("failed consensus, no participation")
							conflict.id = 0
							continue
						}

						if highest.count >= result.answers/2+1 {
							dir.logger.Info(
								"majority reached, conflict solved",
								"owner", highest.mostRecent.GetNodeName(),
								"endpoint_name", highest.mostRecent.GetEndpointName(),
								"mode", highest.mostRecent.GetMode().String(),
							)
						} else {
							highest = nil
							lowestClaimentName := ""
							for k, vote := range votes {
								if lowestClaimentName == "" || k.host < lowestClaimentName {
									highest = vote
									lowestClaimentName = k.host
								}
							}

							dir.logger.Info(
								"majority not reached, solving conflict by tie breaking",
								"owner", highest.mostRecent.GetNodeName(),
								"endpoint_name", highest.mostRecent.GetEndpointName(),
								"mode", highest.mostRecent.GetMode().String(),
							)
						}

						node.active = highest.mostRecent
						node.claimantHistory = nil
						node.lcEdge = time.Time{}
						node.lcLevel = ""
						delete(dir.activeConflicts, epName)
					} else {
						dir.logger.Warn(
							"cannot fix an endpoint conflict alone, initiating a cluster vote",
							"endpoint_name", epName,
						)
						id, err := dir.gossip.askConsensus(epName)
						if err != nil {
							dir.logger.Error("could not init a vote, retry next tick", LabelError.L(err))
							continue
						}

						conflict.id = id
					}
				}
			}
			dir.lk.Unlock()
		case <-dir.closeCh:
			return
		}
	}
}

func (dir *nameDirectory) close() {
	close(dir.closeCh)
}
