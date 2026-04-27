package main

import (
	"log"

	"github.com/NirajNair/lsm-db/internal/db"
)

func main() {
	db, err := db.NewDb[string, string](1)
	if err != nil {
		log.Fatalf("Failed to create DB: %v", err)
	}
	db.Put("a", "apple")
	db.Put("b", "ball")
	val, _ := db.Get("a")
	log.Printf("Get('a') = %s (should be 'apple')", val)
	val, _ = db.Get("b")
	log.Printf("Get('b') = %s (should be 'ball')", val)
}
