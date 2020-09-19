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
)

const (
	concurrentWorkers = 1024
)

var (
	collections = map[int]string{
		bmp.PeerStateChangeMsg: "Node_Test",
		bmp.LSLinkMsg:          "LSLink_Test",
		bmp.LSNodeMsg:          "LSNode_Test",
		bmp.LSPrefixMsg:        "LSPrefix_Test",
		bmp.LSSRv6SIDMsg:       "LSSRv6SID_Test",
		//		bmp.L3VPNMsg:           "",
		bmp.UnicastPrefixMsg: "UnicastPrefix_Test",
	}
)

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
	for t, n := range collections {
		if err := arango.ensureCollection(n, t); err != nil {
			return nil, err
		}
	}

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
		case bmp.PeerStateChangeMsg:
			a.collections[collectionType].handler = a.collections[collectionType].peerStateChangeHandler
		case bmp.LSLinkMsg:
			a.collections[collectionType].handler = a.collections[collectionType].lsLinkHandler
		case bmp.LSNodeMsg:
			a.collections[collectionType].handler = a.collections[collectionType].lsNodeHandler
		case bmp.LSPrefixMsg:
			a.collections[collectionType].handler = a.collections[collectionType].lsPrefixHandler
		case bmp.LSSRv6SIDMsg:
			a.collections[collectionType].handler = a.collections[collectionType].lsSRv6SIDHandler
			//		case bmp.L3VPNMsg:
		case bmp.UnicastPrefixMsg:
			a.collections[collectionType].handler = a.collections[collectionType].genericHandler
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
