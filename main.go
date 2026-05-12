package main

import (
	"fmt"
	"log"

	"github.com/NirajNair/lsm-db/internal/db"
)

func main() {
	// Use a small memtable size (2 bytes) so that every Put triggers a flush,
	// making it easy to trigger L0 compaction (needs ≥4 SSTable files).
	database, err := db.NewDB(2)
	if err != nil {
		log.Fatalf("Failed to create DB: %v", err)
	}
	defer database.Close()

	// Insert 8 keys — each Put should trigger a flush (memtable size = 2).
	// After 4 flushes, L0 compaction should trigger (L0→L1).
	// After 8 flushes, another L0 compaction, and potentially L1→L2 cascade.
	keys := []string{"a", "b", "c", "d", "e", "f", "g", "h"}
	vals := []string{"apple", "ball", "cat", "dog", "elephant", "fish", "grape", "horse"}

	for i := range keys {
		if err := database.Put([]byte(keys[i]), []byte(vals[i])); err != nil {
			log.Fatalf("Failed to PUT key=%s: %v", keys[i], err)
		}
		fmt.Printf("Put('%s'='%s')\n", keys[i], vals[i])
	}

	// Verify all keys are still readable after compaction.
	fmt.Println("\n--- Verification ---")
	for i := range keys {
		val, err := database.Get([]byte(keys[i]))
		if err != nil {
			fmt.Printf("Get('%s') = ERROR: %v, expected '%s'\n", keys[i], err, vals[i])
		} else {
			fmt.Printf("Get('%s') = '%s' (expected '%s') %s\n",
				keys[i], string(val), vals[i],
				map[bool]string{true: "✓", false: "✗"}[string(val) == vals[i]])
		}
	}

	// Test delete + compaction.
	fmt.Println("\n--- Delete 'b' and verify ---")
	database.Delete([]byte("b"))
	_, err = database.Get([]byte("b"))
	if err != nil {
		fmt.Printf("Get('b') after delete = ERROR: %v (expected error) ✓\n", err)
	} else {
		fmt.Println("Get('b') after delete = found value (unexpected!) ✗")
	}

	fmt.Println("\n--- Done ---")
}