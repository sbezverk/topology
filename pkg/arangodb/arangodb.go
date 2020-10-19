package arangodb

import (
	"context"
	"fmt"

	driver "github.com/arangodb/go-driver"
	"github.com/golang/glog"
	"github.com/sbezverk/gobmp/pkg/bmp"
	"github.com/sbezverk/gobmp/pkg/tools"
	"github.com/sbezverk/topology/pkg/dbclient"
	"github.com/sbezverk/topology/pkg/kafkanotifier"
)

const (
	concurrentWorkers = 1024
)

// collectionProperties defines a collection specific properties
// TODO this information should be configurable without recompiling code.
type collectionProperties struct {
	name     string
	isVertex bool
	options  *driver.CreateCollectionOptions
}

var (
	collections = map[int]*collectionProperties{
		bmp.PeerStateChangeMsg: {name: "Node_Test", isVertex: false, options: &driver.CreateCollectionOptions{}},
		bmp.LSLinkMsg:          {name: "LSLink_Test", isVertex: false, options: &driver.CreateCollectionOptions{}},
		bmp.LSNodeMsg:          {name: "LSNode_Test", isVertex: true, options: &driver.CreateCollectionOptions{}},
		bmp.LSPrefixMsg:        {name: "LSPrefix_Test", isVertex: false, options: &driver.CreateCollectionOptions{}},
		bmp.LSSRv6SIDMsg:       {name: "LSSRv6SID_Test", isVertex: false, options: &driver.CreateCollectionOptions{}},
		bmp.L3VPNMsg:           {name: "L3VPN_Prefix_Test", isVertex: false, options: &driver.CreateCollectionOptions{}},
		bmp.UnicastPrefixMsg:   {name: "UnicastPrefix_Test", isVertex: false, options: &driver.CreateCollectionOptions{}},
	}
)

type arangoDB struct {
	dbclient.DB
	*ArangoConn
	stop             chan struct{}
	collections      map[int]*collection
	notifyCompletion bool
	notifier         kafkanotifier.Event
}

// NewDBSrvClient returns an instance of a DB server client process
func NewDBSrvClient(arangoSrv, user, pass, dbname string, notifier kafkanotifier.Event) (dbclient.Srv, error) {
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
	if notifier != nil {
		arango.notifyCompletion = true
		arango.notifier = notifier
	}
	// Init collections
	for t, n := range collections {
		if err := arango.ensureCollection(n, t); err != nil {
			return nil, err
		}
	}

	return arango, nil
}

func (a *arangoDB) ensureCollection(p *collectionProperties, collectionType int) error {
	if _, ok := a.collections[collectionType]; !ok {
		a.collections[collectionType] = &collection{
			queue:          make(chan *queueMsg),
			stats:          &stats{},
			stop:           a.stop,
			arango:         a,
			collectionType: collectionType,
			properties:     p,
		}
		switch collectionType {
		case bmp.PeerStateChangeMsg:
			a.collections[collectionType].handler = a.collections[collectionType].genericHandler
		case bmp.LSLinkMsg:
			a.collections[collectionType].handler = a.collections[collectionType].genericHandler
		case bmp.LSNodeMsg:
			a.collections[collectionType].handler = a.collections[collectionType].genericHandler
		case bmp.LSPrefixMsg:
			a.collections[collectionType].handler = a.collections[collectionType].genericHandler
		case bmp.LSSRv6SIDMsg:
			a.collections[collectionType].handler = a.collections[collectionType].genericHandler
		case bmp.L3VPNMsg:
			a.collections[collectionType].handler = a.collections[collectionType].genericHandler
		case bmp.UnicastPrefixMsg:
			a.collections[collectionType].handler = a.collections[collectionType].genericHandler
		default:
			return fmt.Errorf("unknown collection type %d", collectionType)
		}
	}
	var ci driver.Collection
	var err error
	// There are two possible collection types, base type and vertex type
	if !a.collections[collectionType].properties.isVertex {
		ci, err = a.db.Collection(context.TODO(), a.collections[collectionType].properties.name)
		if err != nil {
			if !driver.IsArangoErrorWithErrorNum(err, driver.ErrArangoDataSourceNotFound) {
				return err
			}
			ci, err = a.db.CreateCollection(context.TODO(), a.collections[collectionType].properties.name, a.collections[collectionType].properties.options)
		}
	} else {
		graph, err := a.ensureGraph(a.collections[collectionType].properties.name)
		if err != nil {
			return err
		}
		ci, err = graph.VertexCollection(context.TODO(), a.collections[collectionType].properties.name)
		if err != nil {
			if !driver.IsArangoErrorWithErrorNum(err, driver.ErrArangoDataSourceNotFound) {
				return err
			}
			ci, err = graph.CreateVertexCollection(context.TODO(), a.collections[collectionType].properties.name)
		}
	}
	a.collections[collectionType].topicCollection = ci

	return nil
}

func (a *arangoDB) ensureGraph(name string) (driver.Graph, error) {
	var edgeDefinition driver.EdgeDefinition
	edgeDefinition.Collection = name + "_Edge"
	edgeDefinition.From = []string{name}
	edgeDefinition.To = []string{name}

	var options driver.CreateGraphOptions
	options.EdgeDefinitions = []driver.EdgeDefinition{edgeDefinition}
	graph, err := a.db.Graph(context.TODO(), name)
	if err == nil {
		graph.Remove(context.TODO())
		return a.db.CreateGraph(context.TODO(), name, &options)
	}
	if !driver.IsArangoErrorWithErrorNum(err, 1924) {
		return nil, err
	}

	return a.db.CreateGraph(context.TODO(), name, &options)
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
