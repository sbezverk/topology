package arangodb

import (
	"github.com/sbezverk/gobmp/pkg/message"
)

type flowspecArangoMessage struct {
	*message.Flowspec
}

func (fs *flowspecArangoMessage) MakeKey() string {
	return fs.RouterIP
}
