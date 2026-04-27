package main

import (
	"log"

	"github.com/NirajNair/lsm-db/internal/db"
)

func main() {
	db, err := db.NewDb[string, string](2)
	if err != nil {
		log.Fatalf("Failed to create DB: %v", err)
	}
	db.Put("a", "apple")
	db.Put("b", "ball")
	db.Put("c", "cat")
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
