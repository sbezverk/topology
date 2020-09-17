package processor

import (
	"encoding/json"

	"github.com/golang/glog"
	"github.com/sbezverk/gobmp/pkg/bmp"
	"github.com/sbezverk/gobmp/pkg/message"
	"github.com/sbezverk/gobmp/pkg/tools"
	"github.com/sbezverk/topology/pkg/dbclient"
	"go.uber.org/atomic"
)

const (
	bufferSize = 1024
)

var (
	topicList = []int{
		bmp.EVPNMsg,
		bmp.L3VPNMsg,
		bmp.LSLinkMsg,
		bmp.LSNodeMsg,
		bmp.LSPrefixMsg,
		bmp.LSSRv6SIDMsg,
		bmp.PeerStateChangeMsg,
		bmp.UnicastPrefixMsg,
	}
)

// Messenger defines required methonds of a messaging client
type Messenger interface {
	// SendMessage is used by the messanger client to send message to Processor for processing
	SendMessage(msgType int, msg []byte)
}

// Srv defines required method of a processor server
type Srv interface {
	Start() error
	Stop() error
	GetInterface() Messenger
}

type queueMsg struct {
	msgType int
	msgData []byte
}

var _ Messenger = &processor{}

type stats struct {
	total  atomic.Int64
	failed atomic.Int64
}

type topic struct {
	queue chan *queueMsg
	stats *stats
	stop  chan struct{}
}

type processor struct {
	stop chan struct{}
	db   dbclient.DB
	Messenger
	topics map[int]*topic
}

// NewProcessorSrv returns an instance of a processor server
func NewProcessorSrv(client dbclient.DB) Srv {
	p := &processor{
		stop:   make(chan struct{}),
		db:     client,
		topics: make(map[int]*topic),
	}
	p.Messenger = p
	for _, t := range topicList {
		p.topics[t] = &topic{
			queue: make(chan *queueMsg, bufferSize),
			stats: &stats{},
			stop:  p.stop,
		}
	}
	return p
}

func (p *processor) Start() error {
	glog.Info("Starting Topology Processor")
	// for mt, t := range p.topics {
	// 	if t != nil {
	// 				go t.topicWorker(mt, p.db.StoreMessage)
	// 	}
	// }

	return nil
}

func (p *processor) Stop() error {
	for _, t := range p.topics {
		close(t.queue)
	}
	close(p.stop)

	return nil
}

func (p *processor) GetInterface() Messenger {
	return p.Messenger
}

// SendMessage send received message to the corresponding to message type's handler
func (p *processor) SendMessage(msgType int, msg []byte) {
	if t, ok := p.topics[msgType]; ok {
		t.queue <- &queueMsg{
			msgType: msgType,
			msgData: msg,
		}
	}
}

func (t *topic) topicWorker(msgType int, f func(msgType int, msg interface{}) error) {
	for {
		fail := false
		select {
		case <-t.stop:
			return
		case m := <-t.queue:
			// glog.Infof("total: %d messages of type: %d failed: %d", t.stats.total.Add(1), msgType, t.stats.failed.Load())
			switch m.msgType {
			case bmp.PeerStateChangeMsg:
				var o message.PeerStateChange
				if err := json.Unmarshal(m.msgData, &o); err != nil {
					glog.Errorf("failed to unmarshal message of type %d with error: %+v", m.msgType, err)
					fail = true
				}
				if err := f(m.msgType, &o); err != nil {
					glog.Errorf("failed to store message of type: %d in the database with error: %+v", m.msgType, err)
					fail = true
				}
			case bmp.UnicastPrefixMsg:
				var o message.UnicastPrefix
				if err := json.Unmarshal(m.msgData, &o); err != nil {
					glog.Errorf("failed to unmarshal message of type %d with error: %+v", m.msgType, err)
					fail = true
				}
				if err := f(m.msgType, &o); err != nil {
					glog.Errorf("failed to store message of type: %d in the database with error: %+v", m.msgType, err)
					fail = true
				}
			case bmp.LSNodeMsg:
				var o message.LSNode
				if err := json.Unmarshal(m.msgData, &o); err != nil {
					glog.Errorf("failed to unmarshal message data: %s of type %d with error: %+v", tools.MessageHex(m.msgData), m.msgType, err)
					fail = true
				}
				if err := f(m.msgType, &o); err != nil {
					glog.Errorf("failed to store message of type: %d in the database with error: %+v", m.msgType, err)
					fail = true
				}
			case bmp.LSLinkMsg:
				var o message.LSLink
				if err := json.Unmarshal(m.msgData, &o); err != nil {
					glog.Errorf("failed to unmarshal message of type %d with error: %+v", m.msgType, err)
					fail = true
				}
				if err := f(m.msgType, &o); err != nil {
					glog.Errorf("failed to store message of type: %d in the database with error: %+v", m.msgType, err)
					fail = true
				}
			case bmp.LSPrefixMsg:
				var o message.LSPrefix
				if err := json.Unmarshal(m.msgData, &o); err != nil {
					glog.Errorf("failed to unmarshal message of type %d with error: %+v", m.msgType, err)
					fail = true
				}
				if err := f(m.msgType, &o); err != nil {
					glog.Errorf("failed to store message of type: %d in the database with error: %+v", m.msgType, err)
					fail = true
				}
			case bmp.LSSRv6SIDMsg:
				var o message.LSSRv6SID
				if err := json.Unmarshal(m.msgData, &o); err != nil {
					glog.Errorf("failed to unmarshal message of type %d with error: %+v", m.msgType, err)
					fail = true
				}
				if err := f(m.msgType, &o); err != nil {
					glog.Errorf("failed to store message of type: %d in the database with error: %+v", m.msgType, err)
					fail = true
				}
			case bmp.L3VPNMsg:
				var o message.L3VPNPrefix
				if err := json.Unmarshal(m.msgData, &o); err != nil {
					glog.Errorf("failed to unmarshal message of type %d with error: %+v", m.msgType, err)
					fail = true
				}
				if err := f(m.msgType, &o); err != nil {
					glog.Errorf("failed to store message of type: %d in the database with error: %+v", m.msgType, err)
					fail = true
				}
			case bmp.EVPNMsg:
				var o message.EVPNPrefix
				if err := json.Unmarshal(m.msgData, &o); err != nil {
					glog.Errorf("failed to unmarshal message of type %d with error: %+v", m.msgType, err)
					fail = true
				}
				if err := f(m.msgType, &o); err != nil {
					glog.Errorf("failed to store message of type: %d in the database with error: %+v", m.msgType, err)
					fail = true
				}
			}
		}
		if fail {
			t.stats.failed.Add(1)
		}
	}
}
