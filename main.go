package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/dgraph-io/badger"
)

// Setting version/buildtime
var (
	scoreFile string
	createDb  bool
	zipcode   string
)

// check for numeric only
func isNumeric(s string) bool {
	_, err := strconv.ParseFloat(s, 64)
	return err == nil
}

func main() {
	flag.StringVar(&scoreFile, "c", "", "Score file for somefile.csv")
	flag.StringVar(&zipcode, "z", "", "Zipcode to pull the score")
	flag.BoolVar(&createDb, "create", false, "Create BadgerDB")
	flag.Parse()

	//
	// initialize BadgerDB
	//
	db, err := badger.Open(badger.DefaultOptions("qarikRiskScores"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// if set to create BadgerDB
	if createDb {
		var fileName string
		if len(scoreFile) > 0 {
			fileName = scoreFile
		} else {
			fileName = "scores.csv"
		}

		// Open file handler
		f, err := os.OpenFile(fileName, os.O_RDONLY, os.ModePerm)
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
	}

	// Pull score per zipcode
	if len(zipcode) > 0 {
		err := db.View(func(txn *badger.Txn) error {
			item, err := txn.Get([]byte(zipcode))
			if err != nil {
				log.Printf("Error get record: %v", err)
			} else {
				var valCopy []byte
				err = item.Value(func(val []byte) error {

					// Accessing val here is valid.
					// fmt.Printf("The answer is: %s\n", val)

					// Copying or parsing val is valid.
					valCopy = append([]byte{}, val...)

					return nil
				})
				if err != nil {
					log.Printf("Error to get value: %v", err)
				}

				// You must copy it to use it outside item.Value(...).
				// valCopy, _ = item.ValueCopy(nil)
				fmt.Printf("The answer is: %s\n", valCopy)
			}
			return nil
		})
		if err != nil {
			log.Printf("Error view record: %v", err)
		}
	}
}
