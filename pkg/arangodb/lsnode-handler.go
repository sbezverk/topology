package arangodb

import (
	"strconv"

	"github.com/sbezverk/gobmp/pkg/message"
)

type lsNodeArangoMessage struct {
	*message.LSNode
}

func (n *lsNodeArangoMessage) MakeKey() string {
	// return n.RouterIP + "_" + n.PeerIP
	return strconv.Itoa(int(n.ProtocolID)) + "" + strconv.Itoa(int(n.DomainID)) + "" + n.OSPFAreaID + "_" + n.IGPRouterID
}
