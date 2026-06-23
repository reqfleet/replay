package main

import (
	"fmt"
	"log/slog"
	"net/http"
	"os"
)

func main() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	fmt.Println("Test server listening on localhost:6000")
	if err := http.ListenAndServe("localhost:6000", nil); err != nil {
		slog.Error("Server failed", "error", err)
		os.Exit(1)
	}
}
