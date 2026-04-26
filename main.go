package main

import (
	"log"

	"github.com/NirajNair/lsm-db/internal/db"
)

func main() {
	db, err := db.NewDb[string, string]()
	if err != nil {
		log.Fatalf("Failed to create DB: %v", err)
	}
	db.Put("a", "apple")
	val, _ := db.Get("a")
	log.Printf("Get('a') = %s (should be 'apple')", val)
}
