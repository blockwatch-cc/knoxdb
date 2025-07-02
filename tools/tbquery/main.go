// Copyright (c) 2025 Blockwatch Data Inc.
// Author: oliver@blockwatch.cc

// Command-line tool to query .knox container files for TigerBeetle accounts/transfers
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"blockwatch.cc/knoxdb/internal/pack"
	"blockwatch.cc/knoxdb/pkg/containers"
)

var (
	filePath = flag.String("file", "", "Path to .knox file")
	typeName = flag.String("type", "", "Container type: account | transfer")
	jsonOut  = flag.Bool("json", false, "Print results in JSON format")
	idFilter = flag.String("id", "", "Optional: filter by ID (format: lo:hi)")
)

func main() {
	flag.Parse()

	if *filePath == "" || *typeName == "" {
		log.Fatalf("Missing required --file or --type flags")
	}

	// ✅ Register container types
	containers.RegisterAll()

	r, err := os.Open(*filePath)
	if err != nil {
		log.Fatalf("Failed to open file: %v", err)
	}
	defer r.Close()

	switch strings.ToLower(*typeName) {
	case "account":
		queryAccounts(r)
	case "transfer":
		queryTransfers(r)
	default:
		log.Fatalf("Unknown container type: %s", *typeName)
	}
}

func queryAccounts(f *os.File) {
	var accs []containers.AccountContainer
	err := pack.DecodeContainerReader(f, &accs)
	if err != nil {
		log.Fatalf("Decode failed: %v", err)
	}

	printObjects(accs)
}

func queryTransfers(f *os.File) {
	var transfers []containers.Transfer
	err := pack.DecodeContainerReader(f, &transfers)
	if err != nil {
		log.Fatalf("Decode failed: %v", err)
	}

	if *idFilter != "" {
		parts := strings.Split(*idFilter, ":")
		if len(parts) != 2 {
			log.Fatalf("Invalid ID format, expected lo:hi")
		}
		var lo, hi uint64
		fmt.Sscanf(parts[0], "%x", &lo)
		fmt.Sscanf(parts[1], "%x", &hi)
		id := [2]uint64{lo, hi}

		for _, t := range transfers {
			if t.ID == id {
				printObject(t)
				return
			}
		}
		log.Println("No match for ID")
		return
	}

	printObjects(transfers)
}

func printObjects[T any](objs []T) {
	for _, obj := range objs {
		printObject(obj)
	}
	log.Printf("Total: %d\n", len(objs))
}

func printObject[T any](obj T) {
	if *jsonOut {
		j, _ := json.MarshalIndent(obj, "", "  ")
		fmt.Println(string(j))
	} else {
		fmt.Printf("%+v\n", obj)
	}
}
