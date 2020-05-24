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
	// timedCounter, used to hold and sync counters between cluster members
	timedCounter struct {
		client  http.Client
		members []string
		main    map[string]uint64 // global counter state
		new     map[string]uint64 // local count

		lk  *sync.RWMutex
		nlk *sync.RWMutex
	}

	SyncRequest struct {
		Sec    int         `json:"sec"`
		Values []SyncValue `json:"values"`
	}

	SyncValue struct {
		Id      string `json:"id"`
		Current uint64 `json:"current"`
		New     uint64 `json:"new"`
	}
)

func newCounters(members []string, timeout time.Duration) timedCounter {
	return timedCounter{
		members: members,
		main:    make(map[string]uint64),
		new:     make(map[string]uint64),

		lk:  &sync.RWMutex{},
		nlk: &sync.RWMutex{},
		client: http.Client{
			Timeout: timeout,
		},
	}
}

// read the global and local counters for ID
func (c *timedCounter) get(id string) uint64 {
	c.lk.RLock()
	c.nlk.RLock()
	defer c.nlk.RUnlock()
	defer c.lk.RUnlock()

	return c.main[id] + c.new[id]
}

// increment the counter for the given ID
func (c *timedCounter) inc(id string) {
	c.nlk.Lock()
	defer c.nlk.Unlock()

	log.Println("DEBUG\tincreasing new counter:", id, c.new)

	c.new[id] += 1
}

// apply locally the counter of other cluster member
// the method returns the local counters in case they are bigger
func (c *timedCounter) apply(req SyncRequest) map[string]uint64 {

	if req.Sec > time.Now().Second() {
		log.Println("WARN\ttime has already elapsed. ignoring request")
		return nil
	}

	c.lk.Lock()
	defer c.lk.Unlock()

	log.Println("DEBUG\tincreasing counters:", c.main)
	ret := make(map[string]uint64)

	for _, val := range req.Values {
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

// merging the locally new collected counters with the global ones
func (c *timedCounter) merge() {

	c.lk.Lock()
	c.nlk.Lock()
	defer c.nlk.Unlock()
	defer c.lk.Unlock()

	if len(c.new) == 0 {
		return
	}

	log.Println("DEBUG\tmerging new counters to main:", len(c.new))

	for k, v := range c.new {
		c.main[k] += v
	}

	c.new = make(map[string]uint64)
}

// notifying the other cluster members with the locally collected counters
func (c *timedCounter) notify() error {
	c.lk.Lock()
	c.nlk.Lock()
	defer c.nlk.Unlock()
	defer c.lk.Unlock()

	if len(c.new) == 0 {
		return nil
	}

	log.Println("INFO\tsyncing counters with cluster members", c.members)

	wg := sync.WaitGroup{}
	body, err := json.Marshal(c.prepareRequest())
	if err != nil {
		return err
	}

	for _, member := range c.members {
		wg.Add(1)
		go func(m string) {
			defer wg.Done()
			resp, err := c.client.Post(fmt.Sprintf("http://%s/sync", m), "application/json", bytes.NewReader(body))

			switch {
			case err != nil:
				log.Println("WARN\terror syncing counters:", err)
			case resp.StatusCode == http.StatusOK:
				log.Println("DEBUG\tresponse: ", resp.Status)

			case resp.StatusCode == http.StatusConflict:
				log.Println("WARN\tconflicting response values")
				var m map[string]uint64
				if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
					log.Println("ERROR\terror decoding response:", err)
					return
				}
				c.mergeConflict(m)
			}
		}(member)
	}

	wg.Wait()
	return nil
}

// start periodic tasks for syncing counters between cluster members and resetting the counters when the minute is over
func (c *timedCounter) start(notify time.Duration) {

	tm := time.After(time.Second * time.Duration(60-time.Now().Second()))
	tke := time.NewTicker(10 * time.Second) // initial duration for a ticker for resetting the counters
	tks := time.NewTicker(notify)           // ticker for running the counter notification to all cluster members

	for {
		select {
		case <-tm: //ticker for resetting the periodic 1 min ticker at the beginning of the minute
			log.Println("INFO\tcreating ticker")
			tke.Stop()
			tke = time.NewTicker(time.Minute)
		case <-tke.C:
			c.reset()
		case <-tks.C:
			if err := c.notify(); err != nil {
				log.Println("ERROR\terror syncing counters: ", err)
			}
			c.merge()
		}
	}
}

// resetting the maps we use for collecting the request counts
func (c *timedCounter) reset() {
	c.lk.Lock()
	c.nlk.Lock()
	defer c.nlk.Unlock()
	defer c.lk.Unlock()

	log.Println("INFO\tresetting counters")

	c.main = make(map[string]uint64)
	c.new = make(map[string]uint64)
}

// preparing the request with all new saved counters for for sending to all cluster members
func (c *timedCounter) prepareRequest() SyncRequest {
	req := SyncRequest{
		Sec:    time.Now().Second(),
		Values: nil,
	}

	for k, v := range c.new {
		req.Values = append(req.Values, SyncValue{
			Id:      k,
			Current: c.main[k],
			New:     v,
		})
	}

	return req
}

// mergeConflict is used for merging counters when the other cluster member has a larger counter than us.
// in this case, we set our own counter with that value
func (c *timedCounter) mergeConflict(m map[string]uint64) {
	log.Println("INFO\tmerging conflicted counters to main:", len(m))

	for k, v := range m {
		if c.main[k] < v {
			c.main[k] = v
		}
	}

	c.new = make(map[string]uint64)
}
