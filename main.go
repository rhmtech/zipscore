package main

import (
	"bufio"
	"flag"
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
	flag.StringVar(&scoreFile, "c", "", "Score file for somefile.csv.")
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

}
