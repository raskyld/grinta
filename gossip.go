package grinta

import (
	"log/slog"

	"github.com/hashicorp/memberlist"
)

type gossip struct {
	logger *slog.Logger
}

func (g *gossip) NotifyJoin(node *memberlist.Node) {
	withLogNode(g.logger, node).Info("peer joined cluster")
}

func (g *gossip) NotifyLeave(node *memberlist.Node) {
	withLogNode(g.logger, node).Info("peer left cluster")
}

func (g *gossip) NotifyUpdate(node *memberlist.Node) {
	withLogNode(g.logger, node).Info("peer updated")
}
