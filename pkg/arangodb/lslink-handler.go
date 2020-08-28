package arangodb

import (
	"context"

	driver "github.com/arangodb/go-driver"
	"github.com/golang/glog"
	"github.com/sbezverk/gobmp/pkg/bgp"
	"github.com/sbezverk/gobmp/pkg/message"
	"github.com/sbezverk/gobmp/pkg/sr"
	"github.com/sbezverk/gobmp/pkg/srv6"
)

const (
	lsLinkCollectionName = "LSLink_Test"
)

// LSLink represents the database record structure for L3VPN Prefix collection
type LSLink struct {
	Key                   string                `json:"_key,omitempty"`
	ID                    string                `json:"_id,omitempty"`
	Rev                   string                `json:"_rev,omitempty"`
	RouterIP              string                `json:"router_ip,omitempty"`
	BaseAttributes        *bgp.BaseAttributes   `json:"base_attrs,omitempty"`
	PeerHash              string                `json:"peer_hash,omitempty"`
	PeerIP                string                `json:"peer_ip,omitempty"`
	PeerASN               int32                 `json:"peer_asn,omitempty"`
	Timestamp             string                `json:"timestamp,omitempty"`
	IGPRouterID           string                `json:"igp_router_id,omitempty"`
	RouterID              string                `json:"router_id,omitempty"`
	RoutingID             string                `json:"routing_id,omitempty"`
	LSID                  uint32                `json:"ls_id,omitempty"`
	Protocol              string                `json:"protocol,omitempty"`
	Nexthop               string                `json:"nexthop,omitempty"`
	MTID                  []uint16              `json:"mt_id,omitempty"`
	LocalLinkID           string                `json:"local_link_id,omitempty"`
	RemoteLinkID          string                `json:"remote_link_id,omitempty"`
	InterfaceIP           string                `json:"intf_ip,omitempty"`
	NeighborIP            string                `json:"nei_ip,omitempty"`
	IGPMetric             uint32                `json:"igp_metric,omitempty"`
	AdminGroup            uint32                `json:"admin_group,omitempty"`
	MaxLinkBW             uint32                `json:"max_link_bw,omitempty"`
	MaxResvBW             uint32                `json:"max_resv_bw,omitempty"`
	UnResvBW              []uint32              `json:"unresv_bw,omitempty"`
	TEDefaultMetric       uint32                `json:"te_default_metric,omitempty"`
	LinkProtection        uint16                `json:"link_protection,omitempty"`
	MPLSProtoMask         uint8                 `json:"mpls_proto_mask,omitempty"`
	SRLG                  []uint32              `json:"srlg,omitempty"`
	LinkName              string                `json:"link_name,omitempty"`
	RemoteNodeHash        string                `json:"remote_node_hash,omitempty"`
	LocalNodeHash         string                `json:"local_node_hash,omitempty"`
	RemoteIGPRouterID     string                `json:"remote_igp_router_id,omitempty"`
	RemoteRouterID        string                `json:"remote_router_id,omitempty"`
	LocalNodeASN          uint32                `json:"local_node_asn,omitempty"`
	RemoteNodeASN         uint32                `json:"remote_node_asn,omitempty"`
	SRv6BGPPeerNodeSID    *srv6.BGPPeerNodeSID  `json:"srv6_bgp_peer_node_sid,omitempty"`
	SRv6ENDXSID           *srv6.EndXSIDTLV      `json:"srv6_endx_sid,omitempty"`
	IsPrepolicy           bool                  `json:"isprepolicy"`
	IsAdjRIBIn            bool                  `json:"is_adj_rib_in"`
	LSAdjacencySID        []*sr.AdjacencySIDTLV `json:"ls_adjacency_sid,omitempty"`
	LinkMSD               string                `json:"link_msd,omitempty"`
	UnidirLinkDelay       uint32                `json:"unidir_link_delay,omitempty"`
	UnidirLinkDelayMinMax []uint32              `json:"unidir_link_delay_min_max,omitempty"`
	UnidirDelayVariation  uint32                `json:"unidir_delay_variation,omitempty"`
	UnidirPacketLoss      uint32                `json:"unidir_packet_loss,omitempty"`
	UnidirResidualBW      uint32                `json:"unidir_residual_bw,omitempty"`
	UnidirAvailableBW     uint32                `json:"unidir_available_bw,omitempty"`
	UnidirBWUtilization   uint32                `json:"unidir_bw_utilization,omitempty"`
}

func (a *arangoDB) lslinkHandler(obj *message.LSLink) {
	ctx := context.TODO()
	if obj == nil {
		glog.Warning("LSPrefix object is nil")
		return
	}
	k := obj.InterfaceIP + "_" + obj.NeighborIP
	r := &LSLink{
		Key:                   k,
		ID:                    lsLinkCollectionName + "/" + k,
		RouterIP:              obj.RouterIP,
		BaseAttributes:        obj.BaseAttributes,
		PeerHash:              obj.PeerHash,
		PeerIP:                obj.PeerIP,
		PeerASN:               obj.PeerASN,
		Timestamp:             obj.Timestamp,
		IGPRouterID:           obj.IGPRouterID,
		RouterID:              obj.RouterID,
		RoutingID:             obj.RoutingID,
		LSID:                  obj.LSID,
		Protocol:              obj.Protocol,
		Nexthop:               obj.Nexthop,
		MTID:                  obj.MTID,
		LocalLinkID:           obj.LocalLinkID,
		RemoteLinkID:          obj.RemoteLinkID,
		InterfaceIP:           obj.InterfaceIP,
		NeighborIP:            obj.NeighborIP,
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
		IsPrepolicy:           obj.IsPrepolicy,
		IsAdjRIBIn:            obj.IsAdjRIBIn,
		LSAdjacencySID:        obj.LSAdjacencySID,
		LinkMSD:               obj.LinkMSD,
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
