package arangodb

import (
	"context"
	"fmt"

	driver "github.com/arangodb/go-driver"
	"github.com/golang/glog"
	"github.com/sbezverk/gobmp/pkg/bmp"
	"github.com/sbezverk/gobmp/pkg/tools"
	"github.com/sbezverk/topology/pkg/dbclient"
	"github.com/sbezverk/topology/pkg/locker"
	"go.uber.org/atomic"
)

type collectionInfo struct {
	name    string
	handler func()
}

type queueMsg struct {
	msgType int
	msgData []byte
}

type stats struct {
	total  atomic.Int64
	failed atomic.Int64
}

type Collection interface {
	Add(string, driver.Collection)
	Delete(string)
	Check(string) (driver.Collection, bool)
}

type collection struct {
	queue           chan *queueMsg
	stats           *stats
	stop            chan struct{}
	lckr            locker.Locker
	topicCollection driver.Collection
	name            string
	collectionType  int
	handler         func()
	arango          *arangoDB
}

type arangoDB struct {
	dbclient.DB
	*ArangoConn
	stop        chan struct{}
	lckr        locker.Locker
	collections map[int]*collection
}

// NewDBSrvClient returns an instance of a DB server client process
func NewDBSrvClient(arangoSrv, user, pass, dbname string) (dbclient.Srv, error) {
	if err := tools.URLAddrValidation(arangoSrv); err != nil {
		return nil, err
	}
	arangoConn, err := NewArango(ArangoConfig{
		URL:      arangoSrv,
		User:     user,
		Password: pass,
		Database: dbname,
	})
	if err != nil {
		return nil, err
	}
	arango := &arangoDB{
		stop:        make(chan struct{}),
		collections: make(map[int]*collection),
	}
	arango.DB = arango
	arango.ArangoConn = arangoConn

	// Init collections
	if err := arango.ensureCollection("UnicastPrefix_Test", bmp.UnicastPrefixMsg); err != nil {
		return nil, err
	}

	// arango.collections[bmp.UnicastPrefixMsg] = &collection{
	// 	queue:  make(chan *queueMsg, 10240),
	// 	name:   "UnicastPrefix_Test",
	// 	stats:  &stats{},
	// 	stop:   arango.stop,
	// 	lckr:   locker.NewLocker(),
	// 	arango: arango.db,
	// }
	// arango.collections[bmp.UnicastPrefixMsg].handler = arango.collections[bmp.UnicastPrefixMsg].unicastPrefixHandler
	// c, err := arango.db.Collection(context.TODO(), arango.collections[bmp.UnicastPrefixMsg].name)
	// if err != nil {
	// 	if !driver.IsArangoErrorWithErrorNum(err, driver.ErrArangoDataSourceNotFound) {
	// 		return nil, err
	// 	}
	// 	c, err = arango.db.CreateCollection(context.TODO(), arango.collections[bmp.UnicastPrefixMsg].name, &driver.CreateCollectionOptions{})
	// }
	// arango.collections[bmp.UnicastPrefixMsg].topicCollection = c

	return arango, nil
}

func (a *arangoDB) ensureCollection(name string, collectionType int) error {
	if _, ok := a.collections[collectionType]; !ok {
		a.collections[collectionType] = &collection{
			queue:          make(chan *queueMsg),
			name:           name,
			stats:          &stats{},
			stop:           a.stop,
			arango:         a,
			collectionType: collectionType,
		}
		switch collectionType {
		case bmp.UnicastPrefixMsg:
			a.collections[collectionType].handler = a.collections[collectionType].unicastPrefixHandler
		default:
			return fmt.Errorf("unknown collection type %d", collectionType)
		}
	}
	ci, err := a.db.Collection(context.TODO(), a.collections[collectionType].name)
	if err != nil {
		if !driver.IsArangoErrorWithErrorNum(err, driver.ErrArangoDataSourceNotFound) {
			return err
		}
		ci, err = a.db.CreateCollection(context.TODO(), a.collections[collectionType].name, &driver.CreateCollectionOptions{})
	}
	a.collections[collectionType].topicCollection = ci

	return nil
}

func (a *arangoDB) Start() error {
	glog.Infof("Connected to arango database, starting monitor")
	go a.monitor()
	for _, c := range a.collections {
		go c.handler()
	}
	return nil
}

func (a *arangoDB) Stop() error {
	close(a.stop)

	return nil
}

func (a *arangoDB) GetInterface() dbclient.DB {
	return a.DB
}

func (a *arangoDB) GetArangoDBInterface() *ArangoConn {
	return a.ArangoConn
}

func (a *arangoDB) StoreMessage(msgType int, msg []byte) error {
	if t, ok := a.collections[msgType]; ok {
		t.queue <- &queueMsg{
			msgType: msgType,
			msgData: msg,
		}
	}
	// switch msgType {
	// case bmp.PeerStateChangeMsg:
	// 	p, ok := msg.(*message.PeerStateChange)
	// 	if !ok {
	// 		return fmt.Errorf("malformed PeerStateChange message")
	// 	}
	// 	a.peerChangeHandler(p)
	// case bmp.UnicastPrefixMsg:
	// 	un, ok := msg.(*message.UnicastPrefix)
	// 	if !ok {
	// 		return fmt.Errorf("malformed UnicastPrefix message")
	// 	}
	// 	return a.unicastPrefixHandler(un)
	// case bmp.LSLinkMsg:
	// 	lsl, ok := msg.(*message.LSLink)
	// 	if !ok {
	// 		return fmt.Errorf("malformed LSNode message")
	// 	}
	// 	a.lslinkHandler(lsl)
	// case bmp.LSNodeMsg:
	// 	lsn, ok := msg.(*message.LSNode)
	// 	if !ok {
	// 		return fmt.Errorf("malformed LSNode message")
	// 	}
	// 	a.lsnodeHandler(lsn)
	// case bmp.LSPrefixMsg:
	// 	lsp, ok := msg.(*message.LSPrefix)
	// 	if !ok {
	// 		return fmt.Errorf("malformed LSPrefix message")
	// 	}
	// 	a.lsprefixHandler(lsp)
	// case bmp.LSSRv6SIDMsg:
	// 	srv6sid, ok := msg.(*message.LSSRv6SID)
	// 	if !ok {
	// 		return fmt.Errorf("malformed LSPrefix message")
	// 	}
	// 	a.lsSRV6SIDHandler(srv6sid)
	// case bmp.L3VPNMsg:
	// 	l3, ok := msg.(*message.L3VPNPrefix)
	// 	if !ok {
	// 		return fmt.Errorf("malformed L3VPN message")
	// 	}
	// 	a.l3vpnHandler(l3)
	// }

	return nil
}

func (a *arangoDB) monitor() {
	for {
		select {
		case <-a.stop:
			// TODO Add clean up of connection with Arango DB
			return
		}
	}
}
