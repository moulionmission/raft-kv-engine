package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/moulionmission/raft-kv-engine/pkg/raft"
)

func main() {
	nodeID := flag.String("id", "node-0", "ID of this node")
	peersFlag := flag.String("peers", "", "Comma-separated peer IDs")
	httpPort := flag.Int("port", 8080, "HTTP port for REST API")
	flag.Parse()

	var peers []string
	if *peersFlag != "" {
		peers = strings.Split(*peersFlag, ",")
	}

	network := raft.NewSimulatedNetwork()
	store := raft.NewKVStore()
	cfg := raft.DefaultConfig(*nodeID, peers)

	node := raft.NewNode(cfg, network, store)
	node.Start()

	// REST API Handler
	http.HandleFunc("/kv", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			key := r.URL.Query().Get("key")
			if key == "" {
				http.Error(w, "missing key parameter", http.StatusBadRequest)
				return
			}
			val, exists := store.Get(key)
			if !exists {
				http.Error(w, "key not found", http.StatusNotFound)
				return
			}
			json.NewEncoder(w).Encode(map[string]string{"key": key, "value": val})

		case http.MethodPost:
			var req struct {
				Key   string `json:"key"`
				Value string `json:"value"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			idx, err := node.SubmitCommand("PUT", req.Key, req.Value)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadGateway)
				return
			}
			w.WriteHeader(http.StatusAccepted)
			json.NewEncoder(w).Encode(map[string]interface{}{"status": "committed", "log_index": idx})

		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	http.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"node_id": *nodeID,
			"role":    node.GetRole(),
			"term":    node.GetTerm(),
		})
	})

	log.Printf("Starting Raft node %s on port %d...", *nodeID, *httpPort)
	if err := http.ListenAndServe(fmt.Sprintf(":%d", *httpPort), nil); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
