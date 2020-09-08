package arangodb

import (
	"context"
	"strings"

	driver "github.com/arangodb/go-driver"
	"github.com/golang/glog"
	"github.com/sbezverk/gobmp/pkg/message"
)

const (
	lsLinkCollectionName = "LSLink_Test"
)

func (a *arangoDB) lslinkHandler(obj *message.LSLink) {
	ctx := context.TODO()
	if obj == nil {
		glog.Warning("LSPrefix object is nil")
		return
	}
	k := strings.Join(obj.LocalLinkIP, "_") + "..." + strings.Join(obj.RemoteLinkIP, "_")
	// Locking the key "k" to prevent race over the same key value
	a.lckr.Lock(k)
	defer a.lckr.Unlock(k)
	r := &message.LSLink{
		Key:                   k,
		ID:                    lsLinkCollectionName + "/" + k,
		RouterIP:              obj.RouterIP,
		PeerHash:              obj.PeerHash,
		PeerIP:                obj.PeerIP,
		PeerASN:               obj.PeerASN,
		Timestamp:             obj.Timestamp,
		IGPRouterID:           obj.IGPRouterID,
		RouterID:              obj.RouterID,
		LSID:                  obj.LSID,
		Protocol:              obj.Protocol,
		Nexthop:               obj.Nexthop,
		MTID:                  obj.MTID,
		LocalLinkID:           obj.LocalLinkID,
		RemoteLinkID:          obj.RemoteLinkID,
		LocalLinkIP:           obj.LocalLinkIP,
		RemoteLinkIP:          obj.RemoteLinkIP,
		IGPMetric:             obj.IGPMetric,
		AdminGroup:            obj.AdminGroup,
		MaxLinkBW:             obj.MaxLinkBW,
		MaxResvBW:             obj.MaxResvBW,
		UnResvBW:              obj.UnResvBW,
		TEDefaultMetric:       obj.TEDefaultMetric,
		LinkProtection:        obj.LinkProtection,
		MPLSProtoMask:         obj.MPLSProtoMask,
		SRLG:                  obj.SRLG,
		LinkName:              obj.LinkName,
		RemoteNodeHash:        obj.RemoteNodeHash,
		LocalNodeHash:         obj.LocalNodeHash,
		RemoteIGPRouterID:     obj.RemoteIGPRouterID,
		RemoteRouterID:        obj.RemoteRouterID,
		LocalNodeASN:          obj.LocalNodeASN,
		RemoteNodeASN:         obj.RemoteNodeASN,
		SRv6BGPPeerNodeSID:    obj.SRv6BGPPeerNodeSID,
		SRv6ENDXSID:           obj.SRv6ENDXSID,
		LSAdjacencySID:        obj.LSAdjacencySID,
		LinkMSD:               obj.LinkMSD,
		AppSpecLinkAttr:       obj.AppSpecLinkAttr,
		UnidirLinkDelay:       obj.UnidirLinkDelay,
		UnidirLinkDelayMinMax: obj.UnidirLinkDelayMinMax,
		UnidirDelayVariation:  obj.UnidirDelayVariation,
		UnidirPacketLoss:      obj.UnidirPacketLoss,
		UnidirResidualBW:      obj.UnidirResidualBW,
		UnidirAvailableBW:     obj.UnidirAvailableBW,
		UnidirBWUtilization:   obj.UnidirBWUtilization,
	}

	var prc driver.Collection
	var err error
	if prc, err = a.ensureCollection(lsLinkCollectionName); err != nil {
		glog.Errorf("failed to ensure for collection %s with error: %+v", lsLinkCollectionName, err)
		return
	}
	ok, err := prc.DocumentExists(ctx, k)
	if err != nil {
		glog.Errorf("failed to check for document %s with error: %+v", k, err)
		return
	}

	switch obj.Action {
	case "add":
		if ok {
			glog.Infof("Update for existing link: %s", k)
			if _, err := prc.UpdateDocument(ctx, k, r); err != nil {
				glog.Errorf("failed to update document %s with error: %+v", k, err)
				return
			}
			// All good, the document was updated and processRouteTargets succeeded, returning...
			return
		}
		glog.Infof("Add new link: %s", k)
		if _, err := prc.CreateDocument(ctx, r); err != nil {
			glog.Errorf("failed to create document %s with error: %+v", k, err)
			return
		}
	case "del":
		if ok {
			glog.Infof("Delete for existing link: %s", k)
			// Document by the key exists, hence delete it
			if _, err := prc.RemoveDocument(ctx, k); err != nil {
				glog.Errorf("failed to delete document %s with error: %+v", k, err)
				return
			}
			return
		}
	}
}
