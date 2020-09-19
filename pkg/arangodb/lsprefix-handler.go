package arangodb

import (
	"context"
	"encoding/json"
	"strconv"

	driver "github.com/arangodb/go-driver"
	"github.com/golang/glog"
	"github.com/sbezverk/gobmp/pkg/message"
)

type lsPrefixArangoMessage struct {
	*message.LSPrefix
}

func (p *lsPrefixArangoMessage) MakeKey() string {
	return p.Prefix + "_" + strconv.Itoa(int(p.PrefixLen)) + "_" + p.IGPRouterID
}

func (c *collection) lsPrefixHandler() {
	glog.Infof("Starting LS Prefix handler...")
	// keyStore is used to track duplicate key in messages, duplicate key means there is already in processing
	// a go routine for the key
	keyStore := make(map[string]bool)
	// backlog is used to store duplicate key entry until the key is released (finished processing)
	backlog := make(map[string]FIFO)
	// tokens are used to control a number of concurrent goroutine accessing the same collection, to prevent
	// conflicting database changes, each go routine processes a message with the unique key.
	tokens := make(chan struct{}, concurrentWorkers)
	done := make(chan *result, concurrentWorkers*2)
	for {
		select {
		case m := <-c.queue:
			var o lsPrefixArangoMessage
			if err := json.Unmarshal(m.msgData, &o); err != nil {
				glog.Errorf("failed to unmarshal LS Prefix message with error: %+v", err)
				continue
			}
			k := o.MakeKey()
			busy, ok := keyStore[k]
			if ok && busy {
				// Check if there is already a backlog for this key, if not then create it
				b, ok := backlog[k]
				if !ok {
					b = newFIFO()
				}
				// Saving message in the backlog
				b.Push(&o)
				backlog[k] = b
				continue
			}
			// Depositing one token and calling worker to process message for the key
			tokens <- struct{}{}
			keyStore[k] = true
			go c.lsPrefixWorker(k, &o, done, tokens)
		case r := <-done:
			if r.err != nil {
				// Error was encountered during processing of the key
				if c.processError(r) {
					glog.Errorf("lsPrefixWorker for key: %s reported a fatal error: %+v", r.key, r.err)
				}
				glog.Errorf("lsPrefixWorker for key: %s reported a non fatal error: %+v", r.key, r.err)
			}
			delete(keyStore, r.key)
			// Check if there an entry for this key in the backlog, if there is, retrieve it and process it
			b, ok := backlog[r.key]
			if !ok {
				continue
			}
			bo := b.Pop()
			if bo != nil {
				tokens <- struct{}{}
				keyStore[r.key] = true
				go c.lsPrefixWorker(r.key, bo.(*lsPrefixArangoMessage), done, tokens)
			}
			if b.Len() == 0 {
				delete(backlog, r.key)
			}
		case <-c.stop:
			return
		}
	}
}

func (c *collection) lsPrefixWorker(k string, obj *lsPrefixArangoMessage, done chan *result, tokens chan struct{}) {
	var err error
	defer func() {
		<-tokens
		done <- &result{key: k, err: err}
		if err == nil {
			c.stats.total.Add(1)
		}
		glog.V(5).Infof("done key: %s, total messages: %s", k, c.stats.total.String())
	}()
	ctx := context.TODO()
	obj.Key = k
	obj.ID = c.name + "/" + k

	switch obj.Action {
	case "add":
		if glog.V(6) {
			glog.Infof("Add new prefix: %s", obj.Key)
		}
		if _, e := c.topicCollection.CreateDocument(ctx, obj); e != nil {
			switch {
			// The following 2 types of errors inidcate that the document by the key already
			// exists, no need to fail but instead call Update of the document.
			case driver.IsArangoErrorWithErrorNum(e, driver.ErrArangoConflict):
			case driver.IsArangoErrorWithErrorNum(e, driver.ErrArangoUniqueConstraintViolated):
			default:
				err = e
				break
			}
			if _, e := c.topicCollection.UpdateDocument(ctx, obj.Key, obj); e != nil {
				err = e
				break
			}
		}
	case "del":
		if glog.V(6) {
			glog.Infof("Delete for prefix: %s", obj.Key)
		}
		if _, e := c.topicCollection.RemoveDocument(ctx, obj.Key); e != nil {
			if !driver.IsArangoErrorWithErrorNum(e, driver.ErrArangoDocumentNotFound) {
				err = e
			}
		}
	}

	return
}
