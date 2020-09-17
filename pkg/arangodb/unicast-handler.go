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

func (c *collection) unicastPrefixHandler() {
	glog.Infof("Starting Unicast Prefix handler...")
	// keyStore is used to track duplicate key in messages, duplicate key means there is already in processing
	// a go routine for the key
	keyStore := make(map[string]bool)
	// backlog is used to store duplicate key entry until the key is released (finished processing)
	backlog := make(map[string]FIFO)
	tokens := make(chan struct{}, 1024)
	done := make(chan string, 2048)
	for {
		select {
		case m := <-c.queue:
			var o message.UnicastPrefix
			if err := json.Unmarshal(m.msgData, &o); err != nil {
				glog.Errorf("failed to unmarshal Unicast Prefix message with error: %+v", err)
				// workSlot is not used, returning it
				continue
			}
			k := o.Prefix + "_" + strconv.Itoa(int(o.PrefixLen)) + "_" + o.PeerIP
			//			glog.Infof("Received message for key: %s", k)
			busy, ok := keyStore[k]
			if ok && busy {
				// Check if there is already a backlog for this key, if not then create it
				b, ok := backlog[k]
				if !ok {
					b = newUnicastPrefixFIFO()
				}
				// Saving message in the backlog
				b.Push(&o)
				//				glog.Infof("backlog length for key:%s %d", k, b.Len())
				backlog[k] = b
				continue
			}
			// Calling worker to process message for the key
			//			glog.Infof("Depositing one token")
			tokens <- struct{}{}
			keyStore[k] = true
			//			glog.Infof("Deposited one token")
			go c.unicastPrefixWorker(k, &o, done, tokens)
			//			glog.Infof("Started go routine for key: %s", k)
		case k := <-done:
			//			glog.Infof("Received done for key: %s", k)
			delete(keyStore, k)
			// Check if there an entry for this key in the backlog, if there is, retrieve it and process it
			b, ok := backlog[k]
			if !ok {
				continue
			}
			bo := b.Pop()
			//			glog.V(6).Infof("processing entry from the backlog for key: %s object: %+v items left in the backlog: %d", k, bo, b.Len())
			if bo != nil {
				// glog.Infof("Depositing one token")
				tokens <- struct{}{}
				keyStore[k] = true
				//				glog.Infof("Deposited one token")
				go c.unicastPrefixWorker(k, bo, done, tokens)
				//				glog.Infof("Started go routine for key: %s", k)
			}
			if b.Len() == 0 {
				delete(backlog, k)
			}
		case <-c.stop:
			return
		}
	}
}

func (c *collection) unicastPrefixWorker(k string, obj *message.UnicastPrefix, done chan string, tokens chan struct{}) {
	//	glog.Infof("worker for key: %s", k)
	defer func() {
		<-tokens
		//		glog.Infof("Returned token used for key: %s", k)
		// Informing Handler that the key has been processed
		done <- k
		//		glog.Infof("Returned key: %s", k)
		glog.Infof("unicast handler received message: %s, total messages: %d", k, c.stats.total.Add(1))
		//		glog.Infof("All done for key: %s", k)
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

	// var prc driver.Collection
	// var err error

	// if prc, err = c.arango.CollectionExists(ctx, c.name); err != nil {
	// 	if prc, err = a.ensureCollection(unicastCollectionName); err != nil {
	// 		return fmt.Errorf("failed to ensure for collection %s with error: %+v", unicastCollectionName, err)
	// 	}
	// }
	switch obj.Action {
	case "add":
		if glog.V(6) {
			glog.Infof("Add new prefix: %s", r.Key)
		}
		if _, err := c.topicCollection.CreateDocument(ctx, r); err != nil {
			switch {
			// Condition when a collection was deleted while the topology was running
			case driver.IsArangoErrorWithErrorNum(err, driver.ErrArangoDataSourceNotFound):
				//				if prc, err = a.ensureCollection(unicastCollectionName); err != nil {
				glog.Errorf("failed to ensure for collection %s with error: %+v", unicastCollectionName, err)
				return
				//				}
				// TODO Consider retry logic if the collection was successfully recreated
				// The following two errors are triggered while adding a document with the key
				// already existing in the database.
			case driver.IsArangoErrorWithErrorNum(err, driver.ErrArangoConflict):
				// glog.Errorf("conflict detected for %+v", *r)
			case driver.IsArangoErrorWithErrorNum(err, driver.ErrArangoUniqueConstraintViolated):
				// glog.Errorf("unique constraint violated for %+v", *r)
			case driver.IsPreconditionFailed(err):
				glog.Errorf("precondition for %+v failed", *r)
				return
			default:
				glog.Errorf("failed to add document %s with error: %+v", r.Key, err)
				return
			}
			if _, err := c.topicCollection.UpdateDocument(ctx, r.Key, r); err != nil {
				glog.Errorf("update failed %s with error: %+v", r.Key, err)
				return
			}
		}
	case "del":
		if glog.V(6) {
			glog.Infof("Delete for prefix: %s", r.Key)
		}
		// Document by the key exists, hence delete it
		if _, err := c.topicCollection.RemoveDocument(ctx, r.Key); err != nil {
			if !driver.IsArangoErrorWithErrorNum(err, driver.ErrArangoDocumentNotFound) {
				glog.Errorf("failed to delete document %s with error: %+v", r.Key, err)
				return
			}
		}
	}

	return
}
