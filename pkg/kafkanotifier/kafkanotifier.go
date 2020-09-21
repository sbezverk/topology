package kafkanotifier

import (
	"encoding/json"
	"fmt"
	"math"
	"net"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
	"github.com/golang/glog"
	"github.com/sbezverk/gobmp/pkg/bmp"
)

// Define constants for each topic name
const (
	peerTopic             = "gobmp.parsed.peer_events"
	unicastMessageTopic   = "gobmp.parsed.unicast_prefix_events"
	lsNodeMessageTopic    = "gobmp.parsed.ls_node_events"
	lsLinkMessageTopic    = "gobmp.parsed.ls_link_events"
	l3vpnMessageTopic     = "gobmp.parsed.l3vpn_events"
	lsPrefixMessageTopic  = "gobmp.parsed.ls_prefix_events"
	lsSRv6SIDMessageTopic = "gobmp.parsed.ls_srv6_sid_events"
	evpnMessageTopic      = "gobmp.parsed.evpn_events"
)

var (
	brockerConnectTimeout = 10 * time.Second
	topicCreateTimeout    = 1 * time.Second
)

var (
	// topics defines a list of topic to initialize and connect,
	// initialization is done as a part of NewKafkaPublisher func.
	topicNames = []string{
		peerTopic,
		unicastMessageTopic,
		lsNodeMessageTopic,
		lsLinkMessageTopic,
		l3vpnMessageTopic,
		lsPrefixMessageTopic,
		lsSRv6SIDMessageTopic,
		evpnMessageTopic,
	}
)

type Message struct {
	TopicType int
	Key       string
	ID        string
	Action    string
}

type Event interface {
	EventNotification(*Message) error
}

type notifier struct {
	broker   *sarama.Broker
	config   *sarama.Config
	producer sarama.SyncProducer
}

func (n *notifier) EventNotification(msg *Message) error {
	switch msg.TopicType {
	case bmp.PeerStateChangeMsg:
		return n.triggerNotification(peerTopic, msg)
	case bmp.UnicastPrefixMsg:
		return n.triggerNotification(unicastMessageTopic, msg)
	case bmp.LSNodeMsg:
		return n.triggerNotification(lsNodeMessageTopic, msg)
	case bmp.LSLinkMsg:
		return n.triggerNotification(lsLinkMessageTopic, msg)
	case bmp.L3VPNMsg:
		return n.triggerNotification(l3vpnMessageTopic, msg)
	case bmp.LSPrefixMsg:
		return n.triggerNotification(lsPrefixMessageTopic, msg)
	case bmp.LSSRv6SIDMsg:
		return n.triggerNotification(lsSRv6SIDMessageTopic, msg)
	case bmp.EVPNMsg:
		return n.triggerNotification(evpnMessageTopic, msg)
	}

	return fmt.Errorf("unknown topic type %d", msg.TopicType)
}

func NewKafkaNotifier(kafkaSrv string) (Event, error) {
	glog.Infof("Initializing Kafka events producer client")
	if err := validator(kafkaSrv); err != nil {
		glog.Errorf("Failed to validate Kafka server address %s with error: %+v", kafkaSrv, err)
		return nil, err
	}
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Version = sarama.V0_11_0_0

	br := sarama.NewBroker(kafkaSrv)
	if err := br.Open(config); err != nil {
		if err != sarama.ErrAlreadyConnected {
			return nil, err
		}
	}
	if err := waitForBrokerConnection(br, brockerConnectTimeout); err != nil {
		glog.Errorf("failed to open connection to the broker with error: %+v\n", err)
		return nil, err
	}
	glog.V(5).Infof("Connected to broker: %s id: %d\n", br.Addr(), br.ID())

	for _, t := range topicNames {
		if err := ensureTopic(br, topicCreateTimeout, t); err != nil {
			return nil, err
		}
	}
	producer, err := sarama.NewSyncProducer([]string{kafkaSrv}, config)
	if err != nil {
		return nil, err
	}
	glog.V(5).Infof("Initialized Kafka Sync producer")

	return &notifier{
		broker:   br,
		config:   config,
		producer: producer,
	}, nil
}

func (n *notifier) triggerNotification(topic string, msg *Message) error {
	k := sarama.ByteEncoder{}
	k = []byte(msg.Key)
	m := sarama.ByteEncoder{}
	m, _ = json.Marshal(&struct {
		ID     string
		Key    string
		Action string
	}{
		ID:     msg.ID,
		Key:    msg.Key,
		Action: msg.Action,
	})
	_, _, err := n.producer.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Key:   k,
		Value: m,
	})

	return err
}

func validator(addr string) error {
	host, port, _ := net.SplitHostPort(addr)
	if host == "" || port == "" {
		return fmt.Errorf("host or port cannot be ''")
	}
	// Try to resolve if the hostname was used in the address
	if ip, err := net.LookupIP(host); err != nil || ip == nil {
		// Check if IP address was used in address instead of a host name
		if net.ParseIP(host) == nil {
			return fmt.Errorf("fail to parse host part of address")
		}
	}
	np, err := strconv.Atoi(port)
	if err != nil {
		return fmt.Errorf("fail to parse port with error: %w", err)
	}
	if np == 0 || np > math.MaxUint16 {
		return fmt.Errorf("the value of port is invalid")
	}
	return nil
}

func ensureTopic(br *sarama.Broker, timeout time.Duration, topicName string) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	tout := time.NewTimer(timeout)
	topic := &sarama.CreateTopicsRequest{
		TopicDetails: map[string]*sarama.TopicDetail{
			topicName: {
				NumPartitions:     1,
				ReplicationFactor: 1,
			},
		},
	}

	for {
		t, err := br.CreateTopics(topic)
		if err != nil {
			return err
		}
		if e, ok := t.TopicErrors[topicName]; ok {
			if e.Err == sarama.ErrTopicAlreadyExists || e.Err == sarama.ErrNoError {
				return nil
			}
			if e.Err != sarama.ErrRequestTimedOut {
				return e
			}
		}
		select {
		case <-ticker.C:
			continue
		case <-tout.C:
			return fmt.Errorf("timeout waiting for topic %s", topicName)
		}
	}
}

func waitForBrokerConnection(br *sarama.Broker, timeout time.Duration) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	tout := time.NewTimer(timeout)
	for {
		ok, err := br.Connected()
		if ok {
			return nil
		}
		if err != nil {
			return err
		}
		select {
		case <-ticker.C:
			continue
		case <-tout.C:
			return fmt.Errorf("timeout waiting for the connection to the broker %s", br.Addr())
		}
	}

}
