package grinta

import (
	"log/slog"
	"os"
	"slices"
	"testing"
	"time"

	grintav1alpha1 "github.com/raskyld/grinta/gen/grinta/v1alpha1"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type MockGossip struct {
	m mock.Mock
}

func (g *MockGossip) askConsensus(epName string) (uint64, error) {
	args := g.m.Called(epName)
	return uint64(args.Int(0)), args.Error(1)
}

func (g *MockGossip) confirmClaim(peer, epName string, lastRev uint64) (uint64, error) {
	args := g.m.Called(peer, epName, lastRev)
	return uint64(args.Int(0)), args.Error(1)
}

func (g *MockGossip) cancel(id uint64) {
	g.m.Called(id)
}

func (g *MockGossip) getConsensusResult(id uint64) (*consensusResult, error) {
	args := g.m.Called(id)
	return args.Get(0).(*consensusResult), args.Error(1)
}

func (g *MockGossip) getClaimResult(id uint64) (*grintav1alpha1.NameOwnershipResponse, error) {
	args := g.m.Called(id)
	return args.Get(0).(*grintav1alpha1.NameOwnershipResponse), args.Error(1)
}

func TestNameDirectory_SynchronousRev(t *testing.T) {
	g1 := MockGossip{}
	t1 := newNameDir(slog.Default(), &g1, "t1")

	ep1 := &grintav1alpha1.NameClaim{}
	ep1.SetEndpointName("ep1")
	ep1.SetNodeName("t1")
	ep1.SetMode(grintav1alpha1.NameClaimMode_NAME_CLAIM_MODE_CLAIM)
	ep1.SetRev(0)

	ep2 := &grintav1alpha1.NameClaim{}
	ep2.SetEndpointName("ep2")
	ep2.SetNodeName("t2")
	ep2.SetMode(grintav1alpha1.NameClaimMode_NAME_CLAIM_MODE_CLAIM)
	ep2.SetRev(5)

	require.NoError(t, t1.record(ep1, true))
	require.Equal(t, uint64(1), ep1.GetRev(), "revision should have been updated")

	require.NoError(t, t1.record(ep2, false))
	require.Equal(t, uint64(5), ep2.GetRev(), "revision should not be updated")
}

func TestNameDirectory_Resolution(t *testing.T) {
	g1 := MockGossip{}
	t1 := newNameDir(slog.Default(), &g1, "t1")

	ep1 := &grintav1alpha1.NameClaim{}
	ep1.SetEndpointName("ep1")
	ep1.SetNodeName("t1")
	ep1.SetMode(grintav1alpha1.NameClaimMode_NAME_CLAIM_MODE_CLAIM)

	ep2 := &grintav1alpha1.NameClaim{}
	ep2.SetEndpointName("ep2")
	ep2.SetNodeName("t2")
	ep2.SetMode(grintav1alpha1.NameClaimMode_NAME_CLAIM_MODE_CLAIM)
	ep2.SetRev(5)

	ep3 := &grintav1alpha1.NameClaim{}
	ep3.SetEndpointName("ep3")
	ep3.SetNodeName("t1")
	ep3.SetMode(grintav1alpha1.NameClaimMode_NAME_CLAIM_MODE_UNCLAIM)

	require.NoError(t, t1.record(ep1, true))
	require.NoError(t, t1.record(ep2, false))
	require.NoError(t, t1.record(ep3, true))

	rest1, err1 := t1.resolve("ep1")
	rest2, err2 := t1.resolve("ep2")
	_, err3 := t1.resolve("ep3")

	require.NoError(t, err1)
	require.NoError(t, err2)

	require.Equal(t, ErrNameResolution, err3, "ep3 has been unclaimed it should not be found")
	require.Equal(t, "t1", rest1, "ep1 is owned by node t1")
	require.Equal(t, "t2", rest2, "ep2 is owned by node t2")
}

func TestNameDirectory_ScanPrefix(t *testing.T) {
	g1 := MockGossip{}
	t1 := newNameDir(slog.Default(), &g1, "t1")

	ep1 := &grintav1alpha1.NameClaim{}
	ep1.SetEndpointName("ep1")
	ep1.SetNodeName("t1")
	ep1.SetMode(grintav1alpha1.NameClaimMode_NAME_CLAIM_MODE_CLAIM)

	ep2 := &grintav1alpha1.NameClaim{}
	ep2.SetEndpointName("ep2")
	ep2.SetNodeName("t2")
	ep2.SetMode(grintav1alpha1.NameClaimMode_NAME_CLAIM_MODE_CLAIM)
	ep2.SetRev(5)

	ep3 := &grintav1alpha1.NameClaim{}
	ep3.SetEndpointName("ep3")
	ep3.SetNodeName("t1")
	ep3.SetMode(grintav1alpha1.NameClaimMode_NAME_CLAIM_MODE_UNCLAIM)

	require.NoError(t, t1.record(ep1, true))
	require.NoError(t, t1.record(ep2, false))
	require.NoError(t, t1.record(ep3, true))

	resolved, err := t1.scan("ep")
	require.NoError(t, err)

	require.Contains(t, resolved, "ep1", "ep1 should be found")
	require.Contains(t, resolved, "ep2", "ep2 should be found")
	require.NotContains(t, resolved, "ep3", "ep3 should not be found")
}

func TestNameDirectory_ConflictAvoided(t *testing.T) {
	g1 := MockGossip{}
	t1 := newNameDir(slog.Default(), &g1, "t1")

	ep1 := &grintav1alpha1.NameClaim{}
	ep1.SetEndpointName("ep1")
	ep1.SetNodeName("t1")
	ep1.SetMode(grintav1alpha1.NameClaimMode_NAME_CLAIM_MODE_CLAIM)

	ep2 := &grintav1alpha1.NameClaim{}
	ep2.SetEndpointName("ep1")
	ep2.SetNodeName("t2")
	ep2.SetMode(grintav1alpha1.NameClaimMode_NAME_CLAIM_MODE_CLAIM)
	ep2.SetRev(5)

	require.NoError(t, t1.record(ep1, true))
	require.Equal(t, ErrNameConflict, t1.record(ep2, true), "the conflict should be avoided in sync mode")
}

func TestNameDirectory_ConflictDetected(t *testing.T) {
	g1 := MockGossip{}
	t1 := newTestableNameDir(slog.Default(), &g1, "t1")

	ep1 := &grintav1alpha1.NameClaim{}
	ep1.SetEndpointName("ep1")
	ep1.SetNodeName("t1")
	ep1.SetMode(grintav1alpha1.NameClaimMode_NAME_CLAIM_MODE_CLAIM)

	ep2 := &grintav1alpha1.NameClaim{}
	ep2.SetEndpointName("ep1")
	ep2.SetNodeName("t2")
	ep2.SetMode(grintav1alpha1.NameClaimMode_NAME_CLAIM_MODE_CLAIM)
	ep2.SetRev(5)

	require.NoError(t, t1.record(ep1, true))
	require.Nil(t, t1.getNamesWithConflict(), "no conflict yet")
	require.NoError(t, t1.record(ep2, false))
	require.Contains(t, t1.getNamesWithConflict(), "ep1", "ep1 has conflicts")
}

func TestNameDirectory_ConflictSolved(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level:     slog.LevelDebug,
		AddSource: true,
	}))

	g1 := MockGossip{}
	t1 := newTestableNameDir(logger, &g1, "t1")

	ep1 := &grintav1alpha1.NameClaim{}
	ep1.SetEndpointName("ep1")
	ep1.SetNodeName("t1")
	ep1.SetMode(grintav1alpha1.NameClaimMode_NAME_CLAIM_MODE_CLAIM)

	ep2 := &grintav1alpha1.NameClaim{}
	ep2.SetEndpointName("ep1")
	ep2.SetNodeName("t2")
	ep2.SetMode(grintav1alpha1.NameClaimMode_NAME_CLAIM_MODE_CLAIM)
	ep2.SetRev(5)

	ep2unclaimed := &grintav1alpha1.NameClaim{}
	ep2unclaimed.SetEndpointName("ep1")
	ep2unclaimed.SetNodeName("t2")
	ep2unclaimed.SetMode(grintav1alpha1.NameClaimMode_NAME_CLAIM_MODE_UNCLAIM)
	ep2unclaimed.SetRev(6)

	require.NoError(t, t1.record(ep1, true))
	require.Nil(t, t1.getNamesWithConflict(), "no conflict yet")
	require.NoError(t, t1.record(ep2, false))
	require.Contains(t, t1.getNamesWithConflict(), "ep1", "ep1 has conflicts")
	// t2 relinquish on the claim an unclaim frame is received...
	require.NoError(t, t1.record(ep2unclaimed, false))
	// ...the local node must see the conflict is now solved.
	require.Eventually(t, func() bool {
		return !slices.Contains(t1.getNamesWithConflict(), "ep1")
	}, 10*time.Second, 200*time.Microsecond)
}

func TestNameDirectory_ConflictTimeout_AskConsensus(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level:     slog.LevelDebug,
		AddSource: true,
	}))

	g1 := MockGossip{}
	t1 := newTestableNameDir(logger, &g1, "t1")

	ep1 := &grintav1alpha1.NameClaim{}
	ep1.SetEndpointName("ep1")
	ep1.SetNodeName("t1")
	ep1.SetMode(grintav1alpha1.NameClaimMode_NAME_CLAIM_MODE_CLAIM)

	ep2 := &grintav1alpha1.NameClaim{}
	ep2.SetEndpointName("ep1")
	ep2.SetNodeName("t2")
	ep2.SetMode(grintav1alpha1.NameClaimMode_NAME_CLAIM_MODE_CLAIM)
	ep2.SetRev(5)

	result := &consensusResult{
		answers:     3,
		clusterSize: 3,
		activePerHost: map[string]*grintav1alpha1.NameClaim{
			"t1": ep1,
			"t2": ep2,
			"t3": ep1,
		},
	}

	var nilCons *consensusResult
	g1.m.On("askConsensus", "ep1").Return(10, nil).Once()
	inprogress := g1.m.On("getConsensusResult", uint64(10)).Return(nilCons, ErrGossipInProgress).Times(10)
	g1.m.On("getConsensusResult", uint64(10)).Return(result, nil).Once().NotBefore(inprogress)

	require.NoError(t, t1.record(ep1, true))
	require.Nil(t, t1.getNamesWithConflict(), "no conflict yet")
	require.NoError(t, t1.record(ep2, false))
	require.Contains(t, t1.getNamesWithConflict(), "ep1", "ep1 has conflicts")

	require.Eventually(t, func() bool {
		return !slices.Contains(t1.getNamesWithConflict(), "ep1")
	}, 10*time.Second, 200*time.Millisecond)
	g1.m.AssertExpectations(t)
}

func TestNameDirectory_ConflictTimeout_NotEnoughParticipant(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level:     slog.LevelDebug,
		AddSource: true,
	}))

	g1 := MockGossip{}
	t1 := newTestableNameDir(logger, &g1, "t1")

	ep1 := &grintav1alpha1.NameClaim{}
	ep1.SetEndpointName("ep1")
	ep1.SetNodeName("t1")
	ep1.SetMode(grintav1alpha1.NameClaimMode_NAME_CLAIM_MODE_CLAIM)

	ep2 := &grintav1alpha1.NameClaim{}
	ep2.SetEndpointName("ep1")
	ep2.SetNodeName("t2")
	ep2.SetMode(grintav1alpha1.NameClaimMode_NAME_CLAIM_MODE_CLAIM)
	ep2.SetRev(5)

	result := &consensusResult{
		answers:     2,
		clusterSize: 3,
		activePerHost: map[string]*grintav1alpha1.NameClaim{
			"t1": ep1,
			"t2": ep2,
		},
	}

	consensusAsked := 0

	g1.m.On("askConsensus", "ep1").Return(10, nil).Run(func(args mock.Arguments) {
		consensusAsked++
	})
	g1.m.On("getConsensusResult", uint64(10)).Return(result, nil)

	require.NoError(t, t1.record(ep1, true))
	require.Nil(t, t1.getNamesWithConflict(), "no conflict yet")
	require.NoError(t, t1.record(ep2, false))
	require.Contains(t, t1.getNamesWithConflict(), "ep1", "ep1 has conflicts")

	// Will chain call consensus until enough participation.
	require.Eventually(t, func() bool {
		return consensusAsked > 3
	}, 10*time.Second, 200*time.Millisecond)
	g1.m.AssertExpectations(t)
}

func TestNameDirectory_ConflictTimeout_NoConsensus(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level:     slog.LevelDebug,
		AddSource: true,
	}))

	g1 := MockGossip{}
	t1 := newTestableNameDir(logger, &g1, "t1")

	ep1 := &grintav1alpha1.NameClaim{}
	ep1.SetEndpointName("ep1")
	ep1.SetNodeName("t1")
	ep1.SetMode(grintav1alpha1.NameClaimMode_NAME_CLAIM_MODE_CLAIM)

	ep2 := &grintav1alpha1.NameClaim{}
	ep2.SetEndpointName("ep1")
	ep2.SetNodeName("t2")
	ep2.SetMode(grintav1alpha1.NameClaimMode_NAME_CLAIM_MODE_CLAIM)
	ep2.SetRev(5)

	ep3 := &grintav1alpha1.NameClaim{}
	ep3.SetEndpointName("ep1")
	ep3.SetNodeName("aaa")
	ep3.SetMode(grintav1alpha1.NameClaimMode_NAME_CLAIM_MODE_CLAIM)
	ep3.SetRev(8)

	result := &consensusResult{
		answers:     3,
		clusterSize: 3,
		activePerHost: map[string]*grintav1alpha1.NameClaim{
			"t1":  ep1,
			"t2":  ep2,
			"aaa": ep3,
		},
	}

	var nilCons *consensusResult
	g1.m.On("askConsensus", "ep1").Return(10, nil).Once()
	inprogress := g1.m.On("getConsensusResult", uint64(10)).Return(nilCons, ErrGossipInProgress).Times(2)
	g1.m.On("getConsensusResult", uint64(10)).Return(result, nil).Once().NotBefore(inprogress)

	require.NoError(t, t1.record(ep1, true))
	require.Nil(t, t1.getNamesWithConflict(), "no conflict yet")
	require.NoError(t, t1.record(ep2, false))
	require.Contains(t, t1.getNamesWithConflict(), "ep1", "ep1 has conflicts")

	require.Eventually(t, func() bool {
		return !slices.Contains(t1.getNamesWithConflict(), "ep1")
	}, 10*time.Second, 200*time.Millisecond)
	g1.m.AssertExpectations(t)

	winner, err := t1.resolve("ep1")
	require.NoError(t, err)
	require.Equal(t, "aaa", winner, "aaa should be the winner since, when no clear consensus, the lexico-smallest is chosen")
}
