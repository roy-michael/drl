package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"strings"
	"time"
)

var (
	addr       = flag.String("addr", "localhost:8000", "address to listen on")
	members    = flag.String("members", "", "addresses of other cluster members")
	maxAllowed = flag.Int("maxAllowed", 500, "the maximum number of allowed requests per minute")
)

type (
	// server the server struct. it embeds the http server and the counter to sync the global request counts
	server struct {
		http.Server
		tc timedCounter
	}
)

func newServer(members []string, maxAllowed int) *server {
	mux := http.NewServeMux()

	s := server{
		Server: http.Server{
			Handler: mux,
			Addr:    *addr,
		},
		tc: newCounters(members, 2*time.Second),
	}

	mux.HandleFunc("/verify", s.verify(maxAllowed))
	mux.HandleFunc("/sync", s.syncState())

	return &s
}

// the endpoint for the load balancer to verify if the given ID should be allowed for proxing the request
// max, the maximum allowed requests
func (s *server) verify(max int) http.HandlerFunc {

	return func(w http.ResponseWriter, r *http.Request) {

		id := r.URL.Query().Get("id")
		if id == "" {
			http.Error(w, "missing client id", http.StatusBadRequest)
			return
		}

		c := s.tc.get(id)
		log.Println("DEBUG\treading counter for id", id, ":", c)
		if c := c; c >= uint64(max) {
			http.Error(w, "request quota exceeded", http.StatusServiceUnavailable)
			return
		}

		s.tc.inc(id)
	}
}

// endpoint for syncing the counter state between cluster members
func (s *server) syncState() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var m SyncRequest
		if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
			log.Println("ERROR\terror unmarshalling:", err)
		}
		log.Println("DEBUG\trequest", m)
		ret := s.tc.apply(m)
		if len(ret) > 0 {
			w.WriteHeader(http.StatusConflict)
			if err := json.NewEncoder(w).Encode(ret); err != nil {
				log.Println("WARN\terror writing response:", err)
			}
		}
	}
}

func (s *server) ListenAndServe() error {
	go s.tc.start(1 * time.Second)
	return s.Server.ListenAndServe()
}

func main() {
	flag.Parse()

	log.Println("INFO\tstarting the distributed rate limiter. listening on", *addr)

	s := newServer(strings.Split(*members, ","), *maxAllowed)

	log.Fatal(s.ListenAndServe())
}
