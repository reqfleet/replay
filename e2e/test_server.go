package main

import (
	"fmt"
	"log"
	"net/http"
)

func main() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	fmt.Println("Test server listening on localhost:6000")
	if err := http.ListenAndServe("localhost:6000", nil); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
