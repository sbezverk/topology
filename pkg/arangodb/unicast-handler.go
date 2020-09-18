package arangodb

import (
	"context"
	"encoding/json"
	"strconv"

	driver "github.com/arangodb/go-driver"
	"github.com/golang/glog"
	"github.com/sbezverk/gobmp/pkg/message"
)

const (
	unicastCollectionName = "UnicastPrefix_Test"
)

type FIFO interface {
	Push(*message.UnicastPrefix)
	Pop() *message.UnicastPrefix
	Len() int
}

type entry struct {
	next     *entry
	previous *entry
	data     *message.UnicastPrefix
}
type fifo struct {
	head *entry
	tail *entry
	len  int
}

func (f *fifo) Push(o *message.UnicastPrefix) {
	// Empty stack case
	e := &entry{
		next: f.tail,
		data: o,
	}
	if f.head == nil && f.tail == nil {
		f.head = e
	}
	f.tail = e
	if f.tail.next != nil {
		f.tail.next.previous = f.tail
	}
	f.len++
}

func (f *fifo) Pop() *message.UnicastPrefix {
	if f.head == nil {
		// Stack is empty
		return nil
	}
	data := f.head.data
	f.head = f.head.previous
	f.len--
	return data
}

func (f *fifo) Len() int {
	return f.len
}
func newUnicastPrefixFIFO() FIFO {
	return &fifo{
		head: nil,
		tail: nil,
	}
}

type result struct {
	key string
	err error
}

func (c *collection) unicastPrefixHandler() {
	glog.Infof("Starting Unicast Prefix handler...")
	// keyStore is used to track duplicate key in messages, duplicate key means there is already in processing
	// a go routine for the key
	keyStore := make(map[string]bool)
	// backlog is used to store duplicate key entry until the key is released (finished processing)
	backlog := make(map[string]FIFO)
	// tokens are used to control a number of concurrent goroutine accessing the same collection, to prevent
	// conflicting database changes, each go routine processes a message with the unique key.
	tokens := make(chan struct{}, 1024)
	done := make(chan *result, 2048)
	for {
		select {
		case m := <-c.queue:
			var o message.UnicastPrefix
			if err := json.Unmarshal(m.msgData, &o); err != nil {
				glog.Errorf("failed to unmarshal Unicast Prefix message with error: %+v", err)
				continue
			}
			k := o.Prefix + "_" + strconv.Itoa(int(o.PrefixLen)) + "_" + o.PeerIP
			busy, ok := keyStore[k]
			if ok && busy {
				// Check if there is already a backlog for this key, if not then create it
				b, ok := backlog[k]
				if !ok {
					b = newUnicastPrefixFIFO()
				}
				// Saving message in the backlog
				b.Push(&o)
				backlog[k] = b
				continue
			}
			// Depositing one token and calling worker to process message for the key
			tokens <- struct{}{}
			keyStore[k] = true
			go c.unicastPrefixWorker(k, &o, done, tokens)
		case r := <-done:
			if r.err != nil {
				// Error was encountered during processing of the key
				if c.processError(r) {
					glog.Errorf("unicastPrefixWorker for key: %s reported a fatal error: %+v", r.key, r.err)
				}
				glog.Errorf("unicastPrefixWorker for key: %s reported a non fatal error: %+v", r.key, r.err)
			}
			delete(keyStore, r.key)
			// Check if there an entry for this key in the backlog, if there is, retrieve it and process it
			b, ok := backlog[r.key]
			if !ok {
				continue
			}
			bo := b.Pop()
			//			glog.V(6).Infof("processing entry from the backlog for key: %s object: %+v items left in the backlog: %d", k, bo, b.Len())
			if bo != nil {
				// glog.Infof("Depositing one token")
				tokens <- struct{}{}
				keyStore[r.key] = true
				//				glog.Infof("Deposited one token")
				go c.unicastPrefixWorker(r.key, bo, done, tokens)
				//				glog.Infof("Started go routine for key: %s", k)
			}
			if b.Len() == 0 {
				delete(backlog, r.key)
			}
		case <-c.stop:
			return
		}
	}
}

func (c *collection) unicastPrefixWorker(k string, obj *message.UnicastPrefix, done chan *result, tokens chan struct{}) {
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
	r := &message.UnicastPrefix{
		Key:            k,
		ID:             unicastCollectionName + "/" + k,
		Sequence:       obj.Sequence,
		Hash:           obj.Hash,
		RouterHash:     obj.RouterHash,
		RouterIP:       obj.RouterIP,
		BaseAttributes: obj.BaseAttributes,
		PeerHash:       obj.PeerHash,
		PeerIP:         obj.PeerIP,
		PeerASN:        obj.PeerASN,
		Timestamp:      obj.Timestamp,
		Prefix:         obj.Prefix,
		PrefixLen:      obj.PrefixLen,
		IsIPv4:         obj.IsIPv4,
		OriginAS:       obj.OriginAS,
		Nexthop:        obj.Nexthop,
		IsNexthopIPv4:  obj.IsNexthopIPv4,
		PathID:         obj.PathID,
		Labels:         obj.Labels,
		IsPrepolicy:    obj.IsPrepolicy,
		IsAdjRIBIn:     obj.IsAdjRIBIn,
		PrefixSID:      obj.PrefixSID,
	}

	switch obj.Action {
	case "add":
		if glog.V(6) {
			glog.Infof("Add new prefix: %s", r.Key)
		}
		if _, e := c.topicCollection.CreateDocument(ctx, r); e != nil {
			switch {
			// The following 2 types of errors inidcate that the document by the key already
			// exists, no need to fail but instead call Update of the document.
			case driver.IsArangoErrorWithErrorNum(e, driver.ErrArangoConflict):
			case driver.IsArangoErrorWithErrorNum(e, driver.ErrArangoUniqueConstraintViolated):
			default:
				err = e
				break
			}
			if _, e := c.topicCollection.UpdateDocument(ctx, r.Key, r); e != nil {
				err = e
				break
			}
		}
	case "del":
		if glog.V(6) {
			glog.Infof("Delete for prefix: %s", r.Key)
		}
		if _, e := c.topicCollection.RemoveDocument(ctx, r.Key); e != nil {
			if !driver.IsArangoErrorWithErrorNum(e, driver.ErrArangoDocumentNotFound) {
				err = e
			}
		}
	}

	return
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
