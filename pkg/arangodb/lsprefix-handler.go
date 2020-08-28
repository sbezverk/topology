package arangodb

import (
	"context"
	"strconv"

	driver "github.com/arangodb/go-driver"
	"github.com/golang/glog"
	"github.com/sbezverk/gobmp/pkg/base"
	"github.com/sbezverk/gobmp/pkg/bgp"
	"github.com/sbezverk/gobmp/pkg/message"
	"github.com/sbezverk/gobmp/pkg/sr"
)

const (
	lsPrefixCollectionName = "LSPrefix_Test"
)

// LSPrefix represents the database record structure for L3VPN Prefix collection
type LSPrefix struct {
	Key             string               `json:"_key,omitempty"`
	ID              string               `json:"_id,omitempty"`
	Rev             string               `json:"_rev,omitempty"`
	RouterIP        string               `json:"router_ip,omitempty"`
	BaseAttributes  *bgp.BaseAttributes  `json:"base_attrs,omitempty"`
	PeerIP          string               `json:"peer_ip,omitempty"`
	PeerASN         int32                `json:"peer_asn,omitempty"`
	Timestamp       string               `json:"timestamp,omitempty"`
	IGPRouterID     string               `json:"igp_router_id,omitempty"`
	RouterID        string               `json:"router_id,omitempty"`
	RoutingID       string               `json:"routing_id,omitempty"`
	LSID            uint32               `json:"ls_id,omitempty"`
	ProtocolID      base.ProtoID         `json:"protocol_id,omitempty"`
	Protocol        string               `json:"protocol,omitempty"`
	Nexthop         string               `json:"nexthop,omitempty"`
	LocalNodeHash   string               `json:"local_node_hash,omitempty"`
	MTID            []uint16             `json:"mt_id,omitempty"`
	OSPFRouteType   uint8                `json:"ospf_route_type,omitempty"`
	IGPFlags        uint8                `json:"igp_flags,omitempty"`
	RouteTag        uint8                `json:"route_tag,omitempty"`
	ExtRouteTag     uint8                `json:"ext_route_tag,omitempty"`
	OSPFFwdAddr     string               `json:"ospf_fwd_addr,omitempty"`
	IGPMetric       uint32               `json:"igp_metric,omitempty"`
	Prefix          string               `json:"prefix,omitempty"`
	PrefixLen       int32                `json:"prefix_len,omitempty"`
	IsPrepolicy     bool                 `json:"isprepolicy"`
	IsAdjRIBIn      bool                 `json:"is_adj_rib_in"`
	LSPrefixSID     []*sr.PrefixSIDTLV   `json:"ls_prefix_sid,omitempty"`
	PrefixAttrFlags base.PrefixAttrFlags `json:"prefix_attr_flags,omitempty"`
}

func (a *arangoDB) lsprefixHandler(obj *message.LSPrefix) {
	ctx := context.TODO()
	if obj == nil {
		glog.Warning("LSPrefix object is nil")
		return
	}
	k := obj.Prefix + "_" + strconv.Itoa(int(obj.PrefixLen))
	r := &LSPrefix{
		Key:             k,
		ID:              lsPrefixCollectionName + "/" + k,
		RouterIP:        obj.RouterIP,
		BaseAttributes:  obj.BaseAttributes,
		PeerIP:          obj.PeerIP,
		PeerASN:         obj.PeerASN,
		Timestamp:       obj.Timestamp,
		IGPRouterID:     obj.IGPRouterID,
		RouterID:        obj.RouterID,
		RoutingID:       obj.RoutingID,
		LSID:            obj.LSID,
		ProtocolID:      obj.ProtocolID,
		Protocol:        obj.Protocol,
		Nexthop:         obj.Nexthop,
		LocalNodeHash:   obj.LocalNodeHash,
		MTID:            obj.MTID,
		OSPFRouteType:   obj.OSPFRouteType,
		IGPFlags:        obj.IGPFlags,
		RouteTag:        obj.RouteTag,
		ExtRouteTag:     obj.ExtRouteTag,
		OSPFFwdAddr:     obj.OSPFFwdAddr,
		IGPMetric:       obj.IGPMetric,
		Prefix:          obj.Prefix,
		PrefixLen:       obj.PrefixLen,
		IsPrepolicy:     obj.IsPrepolicy,
		IsAdjRIBIn:      obj.IsAdjRIBIn,
		LSPrefixSID:     obj.LSPrefixSID,
		PrefixAttrFlags: obj.PrefixAttrFlags,
	}

	var prc driver.Collection
	var err error
	if prc, err = a.ensureCollection(lsPrefixCollectionName); err != nil {
		glog.Errorf("failed to ensure for collection %s with error: %+v", a.l3vpnPrefix, err)
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
			glog.Infof("Update for existing prefix: %s", k)
			if _, err := prc.UpdateDocument(ctx, k, r); err != nil {
				glog.Errorf("failed to update document %s with error: %+v", k, err)
				return
			}
			// All good, the document was updated and processRouteTargets succeeded, returning...
			return
		}
		glog.Infof("Add new prefix: %s", k)
		if _, err := prc.CreateDocument(ctx, r); err != nil {
			glog.Errorf("failed to create document %s with error: %+v", k, err)
			return
		}
	case "del":
		if ok {
			glog.Infof("Delete for existing prefix: %s", k)
			// Document by the key exists, hence delete it
			if _, err := prc.RemoveDocument(ctx, k); err != nil {
				glog.Errorf("failed to delete document %s with error: %+v", k, err)
				return
			}
			return
		}
	}
}
