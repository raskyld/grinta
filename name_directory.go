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
	wg              sync.WaitGroup

	logger        *slog.Logger
	fb            FabricControlPlane
	localNodeName string
}

type nameRecord struct {
	owner   string
	history map[string]*grintav1alpha1.NameClaim
}

type conflictRecord struct {
	deadline time.Time
	query    *ResolveEndpointQuery
}

func newNameDir(logger *slog.Logger, fabric FabricControlPlane, localNodeName string) *nameDirectory {
	dir := &nameDirectory{
		d:               NewTree[*nameRecord](),
		activeConflicts: make(map[string]*conflictRecord),
		logger:          logger,
		conflictTimeout: 10 * time.Second,
		conflictTicker:  time.NewTicker(1 * time.Second),
		closeCh:         make(chan struct{}, 1),
		fb:              fabric,
		localNodeName:   localNodeName,
	}

	dir.wg.Add(1)
	go dir.handleConflicts()

	return dir
}

func newTestableNameDir(logger *slog.Logger, fabric FabricControlPlane, localNodeName string) *nameDirectory {
	dir := &nameDirectory{
		d:               NewTree[*nameRecord](),
		activeConflicts: make(map[string]*conflictRecord),
		logger:          logger,
		conflictTimeout: 3 * time.Second,
		conflictTicker:  time.NewTicker(200 * time.Millisecond),
		closeCh:         make(chan struct{}, 1),
		fb:              fabric,
		localNodeName:   localNodeName,
	}

	dir.wg.Add(1)
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

func (dir *nameDirectory) resolve(name string) (string, *grintav1alpha1.NameClaim, error) {
	dir.lk.RLock()
	defer dir.lk.RUnlock()
	currentNode, has := dir.d.Get(name)
	if !has {
		return "", nil, ErrNameResolution
	}

	if len(currentNode.owner) > 0 {
		return currentNode.owner, currentNode.history[currentNode.owner], nil
	}

	return "", nil, ErrNameResolution
}

func (dir *nameDirectory) scan(prefix string) (found []string, err error) {
	dir.lk.RLock()
	defer dir.lk.RUnlock()
	for name, currentNode := range dir.d.WalkPrefix(prefix) {
		if len(currentNode.owner) > 0 {
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
		// In all case, we update the history of the node.
		history, hasHistory := currentNode.history[claimant]
		if hasHistory {
			if rev > history.GetRev() {
				currentNode.history[claimant] = claim
			}
		} else {
			currentNode.history[claimant] = claim
		}

		// Should never happen, but better be exhaustive.
		if claim.GetMode() == grintav1alpha1.NameClaimMode_NAME_CLAIM_MODE_UNSPECIFIED {
			dir.logger.Warn("unexpected claim received")
			return nil
		}

		// If the unclaim comes from the current owner, we just free the node.
		// If there is a mismatch, it is safe to ignore it since we already stored
		// the unclaim event in the history.
		if claim.GetMode() == grintav1alpha1.NameClaimMode_NAME_CLAIM_MODE_UNCLAIM {
			if currentNode.owner == claim.GetNodeName() {
				currentNode.owner = ""
			}
			return nil
		}

		// If we try to apply a claim on a recently unclaimed name,
		// our local state can be reconciled.
		if len(currentNode.owner) == 0 {
			currentNode.owner = claim.GetNodeName()
		}

		// Idempotency rule: You MAY claim a name you already own.
		if currentNode.owner == claim.GetNodeName() {
			return nil
		}

		// At this point, we have a conflict.

		// If we are in synchronous mode, we can just abort.
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
					LabelEndpointName.L(name),
				)
			} else {
				dir.logger.Debug(
					"time before conflict hard resolution",
					LabelDuration.L(conflict.deadline),
					LabelEndpointName.L(name),
				)
			}
		} else {
			dir.activeConflicts[name] = &conflictRecord{
				deadline: now.Add(dir.conflictTimeout),
			}
			dir.logger.Debug("endpoint name conflict detected", LabelEndpointName.L(name))
		}
	} else {
		record := &nameRecord{
			history: map[string]*grintav1alpha1.NameClaim{
				claimant: claim,
			},
		}

		if claim.GetMode() == grintav1alpha1.NameClaimMode_NAME_CLAIM_MODE_CLAIM {
			record.owner = claimant
		}

		dir.d.Insert(name, record)
	}
	return nil
}

func (dir *nameDirectory) handleConflicts() {
	defer dir.wg.Done()
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
					if conflict.query != nil {
						conflict.query.Close()
					}
					delete(dir.activeConflicts, epName)
					continue
				}

				var uniqueClaim *grintav1alpha1.NameClaim
				resolved := true
				for _, claim := range node.history {
					if claim.GetMode() == grintav1alpha1.NameClaimMode_NAME_CLAIM_MODE_CLAIM {
						if uniqueClaim != nil {
							// we still have two different claimants, conflict
							// is not resolved.
							resolved = false
							break
						}
						uniqueClaim = claim
					}
				}

				if resolved {
					if uniqueClaim != nil {
						node.owner = uniqueClaim.GetNodeName()
					} else {
						node.owner = ""
					}

					dir.logger.Debug(
						"conflict resolved",
						LabelEndpointName.L(epName),
						LabelPeerName.L(node.owner),
					)

					if conflict.query != nil {
						conflict.query.Close()
					}

					delete(dir.activeConflicts, epName)
					continue
				}

				if conflict.deadline.Before(now) {
					if conflict.query != nil {
						if !conflict.query.Finished() {
							continue
						}

						response := <-conflict.query.ResponseCh()
						if response == nil {
							dir.logger.Error("failed consensus, unexpected close, retry next tick")
							conflict.query = nil
							continue
						}

						if response.Error != nil {
							if errors.Is(response.Error, ErrNameResolution) {
								// Nobody claims the name, we can just remove the node.
								dir.d.Delete(epName)
								delete(dir.activeConflicts, epName)
								dir.logger.Debug("endpoint freed after conflict", LabelEndpointName.L(epName))
							} else {
								dir.logger.Error("failed consensus, retry next tick", LabelError.L(response.Error))
								conflict.query = nil
							}
							continue
						}

						node.owner = response.Host
						node.history[response.Host] = response.claim
						delete(dir.activeConflicts, epName)
						dir.logger.Info(
							"endpoint name conflict solved",
							LabelEndpointName.L(epName),
							LabelPeerName.L(response.Host),
						)
					} else {
						dir.logger.Warn(
							"cannot fix an endpoint conflict alone, initiating a cluster vote",
							"endpoint_name", epName,
						)
						rq, err := dir.fb.ResolveEndpoint(ResolveEndpointRequest{
							EndpointName: epName,
						})
						if err != nil {
							dir.logger.Error("could not init a vote, retry next tick", LabelError.L(err))
							continue
						}

						conflict.query = rq
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
	dir.wg.Wait()
}
