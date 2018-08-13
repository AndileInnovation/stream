package redis

import (
	"github.com/mediocregopher/radix.v3"
	log "github.com/sirupsen/logrus"
)

type Publisher struct {
	sentinel *radix.Sentinel //An active sentinel client connection
	master   string          //This is the name of the redis cluster, redis refers to clusters in sentinel as masters
}

// Connect creates a sentinel client. Connects to the given sentinel instance,
// pulls the information for the master, and creates an initial pool of connections
// for the master. The client will automatically replace the pool for any master
// should sentinel decide to fail the master over
func (p *Publisher) Connect(addresses []string, master string) error {
	sentinelClient, err := radix.NewSentinel(master, addresses, nil, nil)
	if err != nil {
		return ConnectionError{err.Error()}
	}
	p.sentinel = sentinelClient
	p.master = master
	return nil
}

// Publish will convert the <data> to string using string(data) and then place the result on the
// redis channel with name <destination>
func (p *Publisher) Publish(destination string, data []byte) error {
	err := p.sentinel.Do(radix.Cmd(nil, "PUBLISH", destination, string(data)))
	if err != nil {
		return PublishingFailed{err.Error()}
	}

	//master, err := p.sentinel.GetMaster(p.master)
	//if err != nil {
	//	return CouldNotGetMaster{err.Error()}
	//}
	//defer p.sentinel.PutMaster(p.master, master)
	//if err := master.Cmd("PUBLISH", destination, string(data)).Err; err != nil {
	//	return PublishingFailed{err.Error()}
	//}
	log.Debug("sent "+destination+" ", string(data))
	return nil
}
