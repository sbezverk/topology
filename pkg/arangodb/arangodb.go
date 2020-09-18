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

var (
	collections = map[int]string{
		bmp.PeerStateChangeMsg: "Node_Test",
		//		bmp.LSLinkMsg:          "",
		//		bmp.LSNodeMsg:          "",
		//		bmp.LSPrefixMsg:        "",
		//		bmp.LSSRv6SIDMsg:       "",
		//		bmp.L3VPNMsg:           "",
		bmp.UnicastPrefixMsg: "UnicastPrefix_Test",
	}
)

type result struct {
	key string
	err error
}

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

func (c *collection) processError(r *result) bool {
	switch {
	// Condition when a collection was deleted while the topology was running
	case driver.IsArangoErrorWithErrorNum(r.err, driver.ErrArangoDataSourceNotFound):
		if err := c.arango.ensureCollection(c.name, c.collectionType); err != nil {
			return true
		}
		return false
	case driver.IsPreconditionFailed(r.err):
		glog.Errorf("precondition for %+v failed", r.key)
		return false
	default:
		glog.Errorf("failed to add document %s with error: %+v", r.key, r.err)
		return true
	}
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
			//		case bmp.LSLinkMsg:
			//		case bmp.LSNodeMsg:
			//		case bmp.LSPrefixMsg:
			//		case bmp.LSSRv6SIDMsg:
			//		case bmp.L3VPNMsg:
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
