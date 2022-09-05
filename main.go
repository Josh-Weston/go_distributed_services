package main

import (
	"log"

	"github.com/josh-weston/proglog/internal/server"
)

func main() {
	port := ":8080"
	srv := server.NewHTTPServer(port)
	log.Printf("Listening on port %s", port)
	log.Fatal(srv.ListenAndServe())
}
