package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

type (
	timedCounter struct {
		members []string
		main    map[string]uint64
		new     map[string]uint64
		tke     *time.Ticker
		tks     *time.Ticker
		client  http.Client
		lk      *sync.RWMutex
		nlk     *sync.RWMutex
	}

	SyncValue struct {
		Id      string `json:"id"`
		Current uint64 `json:"current"`
		New     uint64 `json:"new"`
	}
)

func newCounters(members []string, notify, elapse, timeout time.Duration) timedCounter {
	return timedCounter{
		members: members,
		main:    make(map[string]uint64),
		new:     make(map[string]uint64),
		tke:     time.NewTicker(elapse),
		tks:     time.NewTicker(notify),
		lk:      &sync.RWMutex{},
		nlk:     &sync.RWMutex{},
		client: http.Client{
			Timeout: timeout,
		},
	}
}

func (c *timedCounter) get(id string) uint64 {
	c.lk.RLock()
	c.nlk.RLock()
	defer c.nlk.RUnlock()
	defer c.lk.RUnlock()

	return c.main[id] + c.new[id]
}

func (c *timedCounter) inc(id string) {
	c.nlk.Lock()
	defer c.nlk.Unlock()

	log.Println("increasing counters:", c.main, c.new)

	c.new[id] += 1
}

func (c *timedCounter) apply(vals []SyncValue) map[string]uint64 {
	c.lk.Lock()
	defer c.lk.Unlock()

	log.Println("increasing counters:", c.main)
	ret := make(map[string]uint64)
	for _, val := range vals {
		id := val.Id
		if c.main[id] > val.Current {
			ret[id] = c.main[id]
		} else { // the party member main counter is larger than ours
			c.main[id] = val.Current
		}
		c.main[id] += val.New
	}

	return ret
}

func (c *timedCounter) merge() {

	c.lk.Lock()
	c.nlk.Lock()
	defer c.nlk.Unlock()
	defer c.lk.Unlock()

	if len(c.new) == 0 {
		return
	}

	log.Println("merging new counters to main:", len(c.new))

	for k, v := range c.new {
		c.main[k] += v
	}

	c.new = make(map[string]uint64)
}

func (c *timedCounter) notify() error {
	c.lk.Lock()
	c.nlk.Lock()
	defer c.nlk.Unlock()
	defer c.lk.Unlock()

	if len(c.new) == 0 {
		return nil
	}

	log.Println("syncing counters with cluster members", c.members)

	wg := sync.WaitGroup{}
	body, err := json.Marshal(c.prepareRequest())
	if err != nil {
		return err
	}

	//log.Println("json:", string(body))

	for _, member := range c.members {
		wg.Add(1)
		go func(m string) {
			defer wg.Done()
			resp, err := c.client.Post(fmt.Sprintf("http://%s/sync", m), "application/json", bytes.NewReader(body))

			switch {
			case err != nil:
				log.Println("error syncing counters:", err)
			case resp.StatusCode == http.StatusOK:
				log.Println("response: ", resp.Status)

			case resp.StatusCode == http.StatusConflict:
				log.Println("conflicting response values")
				var m map[string]uint64
				if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
					log.Println("error decoding response:", err)
					return
				}
				c.mergeConflict(m)
			}
		}(member)
	}

	wg.Wait()
	return nil
}

func (c *timedCounter) start() {

	tm := time.After(time.Second * time.Duration(60-time.Now().Second()))

	for {
		select {
		case <-tm:  //ticker for resetting the periodic 1 min ticker at the beginning of the minute
			log.Println("creating ticker")
			c.tke.Stop()
			c.tke = time.NewTicker(time.Minute)
		case <-c.tke.C:
			c.reset()
		case <-c.tks.C:
			if err := c.notify(); err != nil {
				log.Println("error syncing counters: ", err)
			}
			c.merge()
		}
	}
}

func (c *timedCounter) reset() {
	c.lk.Lock()
	defer c.lk.Unlock()
	log.Println("resetting counters")

	c.main = make(map[string]uint64)
}

func (c *timedCounter) prepareRequest() []SyncValue {
	var counters []SyncValue
	for k, v := range c.new {
		counters = append(counters, SyncValue{
			Id:      k,
			Current: c.main[k],
			New:     v,
		})
	}

	return counters

}

func (c *timedCounter) mergeConflict(m map[string]uint64) {
	log.Println("merging conflicted counters to main:", len(m))

	for k, v := range m {
		if c.main[k] < v {
			c.main[k] = v
		}
	}

	c.new = make(map[string]uint64)
}
