package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

var healthCheckServer *http.Server

func startHealthChecks(port int) error {
	m := http.NewServeMux()
	m.HandleFunc("/", healthHandler)
	healthCheckServer = &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: m,
	}
	var err error
	go func() {
		err = healthCheckServer.ListenAndServe()
	}()
	time.Sleep(1 * time.Second)
	return err
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	data := map[string]interface{}{
		"status": "ok",
	}
	d, err := json.Marshal(data)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		logError("healthcheck format error: %v", err)
		return
	}
	if _, err := w.Write(d); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		logError("healthcheck response error: %v", err)
		return
	}
}
