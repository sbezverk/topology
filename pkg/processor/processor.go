package processor

import (
	"encoding/json"

	"github.com/golang/glog"
	"github.com/sbezverk/gobmp/pkg/bmp"
	"github.com/sbezverk/gobmp/pkg/message"
	"github.com/sbezverk/gobmp/pkg/tools"
	"github.com/sbezverk/topology/pkg/dbclient"
)

const (
	// NumberOfWorkers is maximum number of concurrent go routines created by the processor to process mesages.
	NumberOfWorkers = 102400
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

type processor struct {
	stop  chan struct{}
	queue chan *queueMsg
	db    dbclient.DB
	Messenger
}

// NewProcessorSrv returns an instance of a processor server
func NewProcessorSrv(client dbclient.DB) Srv {
	queue := make(chan *queueMsg)
	p := &processor{
		stop:  make(chan struct{}),
		queue: queue,
		db:    client,
	}
	p.Messenger = p

	return p
}

func (p *processor) Start() error {
	glog.Info("Starting Topology Processor")
	go p.msgProcessor()

	return nil
}

func (p *processor) Stop() error {
	close(p.queue)
	close(p.stop)

	return nil
}

func (p *processor) GetInterface() Messenger {
	return p.Messenger
}

func (p *processor) SendMessage(msgType int, msg []byte) {
	p.queue <- &queueMsg{
		msgType: msgType,
		msgData: msg,
	}
}

func (p *processor) msgProcessor() {
	pool := make(chan struct{}, NumberOfWorkers)
	for {
		select {
		case msg := <-p.queue:
			// Writing to Pool channel to reserve a worker slot
			pool <- struct{}{}
			go p.procWorker(msg, pool)
		case <-p.stop:
			return
		}
	}
}

func (p *processor) procWorker(m *queueMsg, pool chan struct{}) {
	defer func() {
		// Reading from Pool channel to release the worker slot
		<-pool
	}()
	switch m.msgType {
	case bmp.PeerStateChangeMsg:
		var o message.PeerStateChange
		if err := json.Unmarshal(m.msgData, &o); err != nil {
			glog.Errorf("failed to unmarshal message of type %d with error: %+v", m.msgType, err)
			return
		}
		if err := p.db.StoreMessage(m.msgType, &o); err != nil {
			glog.Errorf("failed to store message of type: %d in the database with error: %+v", m.msgType, err)
			return
		}
	case bmp.UnicastPrefixMsg:
		var o message.UnicastPrefix
		if err := json.Unmarshal(m.msgData, &o); err != nil {
			glog.Errorf("failed to unmarshal message of type %d with error: %+v", m.msgType, err)
			return
		}
		if err := p.db.StoreMessage(m.msgType, &o); err != nil {
			glog.Errorf("failed to store message of type: %d in the database with error: %+v", m.msgType, err)
			return
		}
	case bmp.LSNodeMsg:
		var o message.LSNode
		if err := json.Unmarshal(m.msgData, &o); err != nil {
			glog.Errorf("failed to unmarshal message data: %s of type %d with error: %+v", tools.MessageHex(m.msgData), m.msgType, err)
			return
		}
		if err := p.db.StoreMessage(m.msgType, &o); err != nil {
			glog.Errorf("failed to store message of type: %d in the database with error: %+v", m.msgType, err)
			return
		}
	case bmp.LSLinkMsg:
		var o message.LSLink
		if err := json.Unmarshal(m.msgData, &o); err != nil {
			glog.Errorf("failed to unmarshal message of type %d with error: %+v", m.msgType, err)
			return
		}
		if err := p.db.StoreMessage(m.msgType, &o); err != nil {
			glog.Errorf("failed to store message of type: %d in the database with error: %+v", m.msgType, err)
			return
		}
	case bmp.LSPrefixMsg:
		var o message.LSPrefix
		if err := json.Unmarshal(m.msgData, &o); err != nil {
			glog.Errorf("failed to unmarshal message of type %d with error: %+v", m.msgType, err)
			return
		}
		if err := p.db.StoreMessage(m.msgType, &o); err != nil {
			glog.Errorf("failed to store message of type: %d in the database with error: %+v", m.msgType, err)
			return
		}
	case bmp.L3VPNMsg:
		var o message.L3VPNPrefix
		if err := json.Unmarshal(m.msgData, &o); err != nil {
			glog.Errorf("failed to unmarshal message of type %d with error: %+v", m.msgType, err)
			return
		}
		if err := p.db.StoreMessage(m.msgType, &o); err != nil {
			glog.Errorf("failed to store message of type: %d in the database with error: %+v", m.msgType, err)
			return
		}
	case bmp.EVPNMsg:
		var o message.EVPNPrefix
		if err := json.Unmarshal(m.msgData, &o); err != nil {
			glog.Errorf("failed to unmarshal message of type %d with error: %+v", m.msgType, err)
			return
		}
		if err := p.db.StoreMessage(m.msgType, &o); err != nil {
			glog.Errorf("failed to store message of type: %d in the database with error: %+v", m.msgType, err)
			return
		}
	}

	glog.V(6).Infof("message of type %d was sent to the database for further processing", m.msgType)
}
