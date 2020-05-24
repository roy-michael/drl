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
	maxAllowed = flag.Int("maxAllowed", 6, "the max...")
)

type (
	server struct {
		http.Server
		c   timedCounter
		max int
	}
)

func newServer(members []string, maxAllowed int) *server {
	mux := http.NewServeMux()

	s := server{
		Server: http.Server{
			Handler: mux,
			Addr:    *addr,
		},
		c: newCounters(members, 1*time.Second, 10*time.Second, 2*time.Second),
	}

	mux.HandleFunc("/verify", s.verify(maxAllowed))
	mux.HandleFunc("/sync", s.syncState())

	return &s
}

func (s *server) verify(max int) http.HandlerFunc {

	return func(w http.ResponseWriter, r *http.Request) {

		log.Println("handling request:", r.URL)

		id := r.URL.Query().Get("id")
		if id == "" {
			http.Error(w, "missing client id", http.StatusBadRequest)
			return
		}

		c := s.c.get(id)
		log.Println("reading counter for id", id, ":", c)
		if c := c; c >= uint64(max) {
			http.Error(w, "request quota exceeded", http.StatusServiceUnavailable)
			return
		}

		s.c.inc(id)
	}
}

func (s *server) syncState() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var m []SyncValue
		if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
			log.Println("error unmarshalling:", err)
		}
		log.Println("request", m)
		ret := s.c.apply(m)
		if len(ret) > 0 {
			w.WriteHeader(http.StatusConflict)
			if err := json.NewEncoder(w).Encode(ret); err != nil {
				log.Println("error writing response:", err)
			}
		}
	}
}

func (s *server) ListenAndServe() error {
	go s.c.start()
	return s.Server.ListenAndServe()
}

func main() {
	flag.Parse()

	log.Println("starting the distributed rate limiter. listening on", *addr)

	s := newServer(strings.Split(*members, ","), *maxAllowed)

	log.Fatal(s.ListenAndServe())
}
