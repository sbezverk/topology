package arangodb

import (
	"context"
	"fmt"
	"strconv"

	driver "github.com/arangodb/go-driver"
	"github.com/golang/glog"
	"github.com/sbezverk/gobmp/pkg/message"
)

const (
	unicastCollectionName = "UnicastPrefix_Test"
)

func (a *arangoDB) unicastPrefixHandler(obj *message.UnicastPrefix) error {
	ctx := context.TODO()
	if obj == nil {
		return fmt.Errorf("UnicastPrefix object is nil")
	}
	k := obj.Prefix + "_" + strconv.Itoa(int(obj.PrefixLen)) + "_" + obj.PeerIP
	// Locking the key "k" to prevent race over the same key value
	//	a.lckr.Lock(k)
	//		defer a.lckr.Unlock(k)
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

	var prc driver.Collection
	var err error
	if prc, err = a.checkCollection(unicastCollectionName); err != nil {
		if prc, err = a.ensureCollection(unicastCollectionName); err != nil {
			return fmt.Errorf("failed to ensure for collection %s with error: %+v", unicastCollectionName, err)
		}
	}
	switch obj.Action {
	case "add":
		glog.V(6).Infof("Add new prefix: %s", r.Key)
		if _, err := prc.CreateDocument(ctx, r); err != nil {
			switch {
			// Condition when a collection was deleted while the topology was running
			case driver.IsArangoErrorWithErrorNum(err, driver.ErrArangoDataSourceNotFound):
				if prc, err = a.ensureCollection(unicastCollectionName); err != nil {
					return fmt.Errorf("failed to ensure for collection %s with error: %+v", unicastCollectionName, err)
				}
				// TODO Consider retry logic if the collection was successfully recreated
				return nil
				// The following two errors are triggered while adding a document with the key
				// already existing in the database.
			case driver.IsArangoErrorWithErrorNum(err, driver.ErrArangoConflict):
				// glog.Errorf("conflict detected for %+v", *r)
			case driver.IsArangoErrorWithErrorNum(err, driver.ErrArangoUniqueConstraintViolated):
				// glog.Errorf("unique constraint violated for %+v", *r)
			case driver.IsPreconditionFailed(err):
				return fmt.Errorf("precondition for %+v failed", *r)
			default:
				return fmt.Errorf("failed to add document %s with error: %+v", r.Key, err)
			}
			if _, err := prc.UpdateDocument(ctx, r.Key, r); err != nil {
				return fmt.Errorf("update failed %s with error: %+v", r.Key, err)
			}
		}
	case "del":
		glog.V(6).Infof("Delete for prefix: %s", r.Key)
		// Document by the key exists, hence delete it
		if _, err := prc.RemoveDocument(ctx, r.Key); err != nil {
			if !driver.IsArangoErrorWithErrorNum(err, driver.ErrArangoDocumentNotFound) {
				return fmt.Errorf("failed to delete document %s with error: %+v", r.Key, err)
			}
		}
	}
	return nil
}
