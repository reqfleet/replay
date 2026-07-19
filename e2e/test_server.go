package main

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"slices"
)

const expectedGeneratedBody = `{"message":"hello"}`

func validateGeneratedRequest(r *http.Request) error {
	if r.Method != http.MethodGet {
		return fmt.Errorf("method = %s, want %s", r.Method, http.MethodGet)
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return fmt.Errorf("read request body: %w", err)
	}
	if got := string(body); got != expectedGeneratedBody {
		return fmt.Errorf("body = %q, want %q", got, expectedGeneratedBody)
	}
	if got, want := r.Header.Get("Content-Type"), "application/json"; got != want {
		return fmt.Errorf("Content-Type = %q, want %q", got, want)
	}
	if got, want := r.Header.Values("X-E2E-Trace"), []string{"one", "two"}; !slices.Equal(got, want) {
		return fmt.Errorf("X-E2E-Trace = %v, want %v", got, want)
	}
	return nil
}

func writeOK(w http.ResponseWriter) {
	w.WriteHeader(http.StatusOK)
	if _, err := io.WriteString(w, "OK"); err != nil {
		slog.Debug("write response", "error", err)
	}
}

func main() {
	http.HandleFunc("/e2e/request-body", func(w http.ResponseWriter, r *http.Request) {
		if err := validateGeneratedRequest(r); err != nil {
			http.Error(w, err.Error(), http.StatusUnprocessableEntity)
			return
		}
		writeOK(w)
	})
	http.HandleFunc("/e2e/response-body", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Add("X-E2E-Response", "one")
		w.Header().Add("X-E2E-Response", "two")
		writeOK(w)
	})
	http.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		writeOK(w)
	})

	fmt.Println("Test server listening on localhost:6000")
	if err := http.ListenAndServe("localhost:6000", nil); err != nil {
		slog.Error("Server failed", "error", err)
		os.Exit(1)
	}
}
