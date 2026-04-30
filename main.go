package main

import (
	"fmt"
	"log"

	"github.com/NirajNair/lsm-db/internal/db"
)

func main() {
	database, err := db.NewDB(2)
	if err != nil {
		log.Fatalf("Failed to create DB: %v", err)
	}
	if err := database.Put([]byte("a"), []byte("apple")); err != nil {
		log.Fatalf("Failed to PUT in DB: %v", err)
	}
	if err := database.Put([]byte("b"), []byte("ball")); err != nil {
		log.Fatalf("Failed to PUT in DB: %v", err)
	}
	if err := database.Put([]byte("c"), []byte("cat")); err != nil {
		log.Fatalf("Failed to PUT in DB: %v", err)
	}

	val, _ := database.Get([]byte("a"))
	fmt.Printf("Get('a') = %s, expected 'apple'\n", val)
	val, _ = database.Get([]byte("b"))
	fmt.Printf("Get('b') = %s, expected 'ball'\n", val)
	val, _ = database.Get([]byte("c"))
	fmt.Printf("Get('c') = %s, expected 'cat'\n", val)
	database.Delete([]byte("b"))
	_, err = database.Get([]byte("b"))
	if err != nil {
		log.Println(err.Error())
	}
}
