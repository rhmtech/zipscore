package main

import (
	"bufio"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/dgraph-io/badger"
)

// check for numeric only
func isNumeric(s string) bool {
	_, err := strconv.ParseFloat(s, 64)
	return err == nil
}

func main() {

	// initialize BadgerDB
	db, err := badger.Open(badger.DefaultOptions("qarikRiskScores"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// total := 0 // count lines
	// begin := time.Now()

	f, err := os.OpenFile("scores.csv", os.O_RDONLY, os.ModePerm)
	if err != nil {
		log.Fatalf("Open file error: %v", err)
		return
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.Split(sc.Text(), ",")
		// if isNumeric(line[0]) && isNumeric(line[1]) {
		// 	total++
		// } else {
		// 	fmt.Printf("Zipcode: %v, Score: %v\n", line[0], line[1])
		// }
		if isNumeric(line[0]) && isNumeric(line[1]) {
			err := db.Update(func(txn *badger.Txn) error {
				err := txn.Set([]byte(line[0]), []byte(line[1]))
				return err
			})

			if err != nil {
				log.Printf("Error committing record: %v", err)
			}
		}

	}
	if err := sc.Err(); err != nil {
		log.Fatalf("Scan error: %v", err)
	}

	// log.Printf("Scanned file, time_used: %v, lines=%v\n", time.Since(begin).Seconds(), total)
}
