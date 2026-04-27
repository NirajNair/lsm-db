package main

import (
	"log"

	"github.com/NirajNair/lsm-db/internal/db"
)

func main() {
	db, err := db.NewDB[string, string](2)
	if err != nil {
		log.Fatalf("Failed to create DB: %v", err)
	}
	if err := db.Put("a", "apple"); err != nil {
		log.Fatalf("Failed to PUT in DB: %v", err)
	}
	if err := db.Put("b", "ball"); err != nil {
		log.Fatalf("Failed to PUT in DB: %v", err)
	}
	if err := db.Put("c", "cat"); err != nil {
		log.Fatalf("Failed to PUT in DB: %v", err)
	}

	val, _ := db.Get("a")
	log.Printf("Get('a') = %s, expected 'apple'", val)
	val, _ = db.Get("b")
	log.Printf("Get('b') = %s, expected 'ball')", val)
	val, _ = db.Get("c")
	log.Printf("Get('c') = %s, expected 'cat')", val)
	db.Delete("b")
	_, err = db.Get("b")
	if err != nil {
		log.Println(err.Error())
	}
}
