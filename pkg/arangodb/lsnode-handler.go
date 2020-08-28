package arangodb

import (
	"context"

	driver "github.com/arangodb/go-driver"
	"github.com/golang/glog"
	"github.com/sbezverk/gobmp/pkg/base"
	"github.com/sbezverk/gobmp/pkg/bgp"
	"github.com/sbezverk/gobmp/pkg/message"
	"github.com/sbezverk/gobmp/pkg/sr"
)

const (
	lsNodeCollectionName = "LSNode_Test"
)

// LSNode represents the database record structure for L3VPN Prefix collection
type LSNode struct {
	Key                 string              `json:"_key,omitempty"`
	ID                  string              `json:"_id,omitempty"`
	Rev                 string              `json:"_rev,omitempty"`
	Sequence            int                 `json:"sequence,omitempty"`
	Hash                string              `json:"hash,omitempty"`
	RouterHash          string              `json:"router_hash,omitempty"`
	RouterIP            string              `json:"router_ip,omitempty"`
	BaseAttributes      *bgp.BaseAttributes `json:"base_attrs,omitempty"`
	PeerHash            string              `json:"peer_hash,omitempty"`
	PeerIP              string              `json:"peer_ip,omitempty"`
	PeerASN             int32               `json:"peer_asn,omitempty"`
	Timestamp           string              `json:"timestamp,omitempty"`
	IGPRouterID         string              `json:"igp_router_id,omitempty"`
	RouterID            string              `json:"router_id,omitempty"`
	RoutingID           string              `json:"routing_id,omitempty"`
	ASN                 uint32              `json:"asn,omitempty"`
	LSID                uint32              `json:"ls_id,omitempty"`
	MTID                []uint16            `json:"mt_id,omitempty"`
	OSPFAreaID          string              `json:"ospf_area_id,omitempty"`
	ISISAreaID          string              `json:"isis_area_id,omitempty"`
	Protocol            string              `json:"protocol,omitempty"`
	ProtocolID          base.ProtoID        `json:"protocol_id,omitempty"`
	Flags               uint8               `json:"flags,omitempty"`
	Nexthop             string              `json:"nexthop,omitempty"`
	Name                string              `json:"name,omitempty"`
	SRCapabilities      *sr.Capability      `json:"ls_sr_capabilities,omitempty"`
	SRAlgorithm         []int               `json:"sr_algorithm,omitempty"`
	SRLocalBlock        *sr.LocalBlock      `json:"sr_local_block,omitempty"`
	SRv6CapabilitiesTLV string              `json:"srv6_capabilities_tlv,omitempty"`
	NodeMSD             string              `json:"node_msd,omitempty"`
	IsPrepolicy         bool                `json:"isprepolicy"`
	IsAdjRIBIn          bool                `json:"is_adj_rib_in"`
}

func (a *arangoDB) lsnodeHandler(obj *message.LSNode) {
	ctx := context.TODO()
	if obj == nil {
		glog.Warning("LSNode object is nil")
		return
	}
	k := obj.RouterIP + "_" + obj.RouterID
	r := &LSNode{
		Key:                 k,
		ID:                  lsNodeCollectionName + "/" + k,
		Sequence:            obj.Sequence,
		Hash:                obj.Hash,
		RouterHash:          obj.RouterHash,
		RouterIP:            obj.RouterIP,
		BaseAttributes:      obj.BaseAttributes,
		PeerHash:            obj.PeerHash,
		PeerIP:              obj.PeerIP,
		PeerASN:             obj.PeerASN,
		Timestamp:           obj.Timestamp,
		IGPRouterID:         obj.IGPRouterID,
		RouterID:            obj.RouterID,
		RoutingID:           obj.RoutingID,
		ASN:                 obj.ASN,
		LSID:                obj.LSID,
		MTID:                obj.MTID,
		OSPFAreaID:          obj.OSPFAreaID,
		ISISAreaID:          obj.ISISAreaID,
		Protocol:            obj.Protocol,
		ProtocolID:          obj.ProtocolID,
		Flags:               obj.Flags,
		Nexthop:             obj.Nexthop,
		Name:                obj.Name,
		SRCapabilities:      obj.SRCapabilities,
		SRAlgorithm:         obj.SRAlgorithm,
		SRLocalBlock:        obj.SRLocalBlock,
		SRv6CapabilitiesTLV: obj.SRv6CapabilitiesTLV,
		NodeMSD:             obj.NodeMSD,
		IsPrepolicy:         obj.IsPrepolicy,
		IsAdjRIBIn:          obj.IsAdjRIBIn,
	}

	var prc driver.Collection
	var err error
	if prc, err = a.ensureCollection(lsNodeCollectionName); err != nil {
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
			glog.Infof("Update for existing node: %s", k)
			if _, err := prc.UpdateDocument(ctx, k, r); err != nil {
				glog.Errorf("failed to update document %s with error: %+v", k, err)
				return
			}
			// All good, the document was updated and processRouteTargets succeeded, returning...
			return
		}
		glog.Infof("Add new node: %s", k)
		if _, err := prc.CreateDocument(ctx, r); err != nil {
			glog.Errorf("failed to create document %s with error: %+v", k, err)
			return
		}
	case "del":
		if ok {
			glog.Infof("Delete for existing node: %s", k)
			// Document by the key exists, hence delete it
			if _, err := prc.RemoveDocument(ctx, k); err != nil {
				glog.Errorf("failed to delete document %s with error: %+v", k, err)
				return
			}
			return
		}
	}
}
