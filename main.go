package main

import (
	"bytes"
	"database/sql"
	"encoding/csv"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"strconv"
	"time"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "gen" {
		rand.Seed(time.Now().UnixNano())
		if err := gen(1000000); err != nil {
			log.Fatal(err)
		}
	}
	{
		t := time.Now()
		if err := importCSV(); err != nil {
			log.Fatal(err)
		}
		fmt.Println(".import csv:", time.Since(t))
	}
	{
		t := time.Now()
		if err := naiveInsert(); err != nil {
			log.Fatal(err)
		}
		fmt.Println("naive insert:", time.Since(t))
	}
	{
		t := time.Now()
		if err := prepareInsert(); err != nil {
			log.Fatal(err)
		}
		fmt.Println("prepare insert:", time.Since(t))
	}
	{
		t := time.Now()
		if err := txInsert(); err != nil {
			log.Fatal(err)
		}
		fmt.Println("tx insert:", time.Since(t))
	}
	{
		t := time.Now()
		if err := txPrepareInsert(); err != nil {
			log.Fatal(err)
		}
		fmt.Println("tx prepare insert:", time.Since(t))
	}
	{
		t := time.Now()
		if err := bulkInsert(); err != nil {
			log.Fatal(err)
		}
		fmt.Println("bulk insert:", time.Since(t))
	}
}

func openTestDB() (*sql.DB, error) {
	x, err := sql.Open("sqlite3", "file:test?mode=memory&cache=shared")
	if err != nil {
		return nil, err
	}
	if _, err := x.Exec(createStmt); err != nil {
		return nil, err
	}
	return x, nil
}

func naiveInsert() error {
	x, err := openTestDB()
	if err != nil {
		return err
	}
	defer x.Close()
	records, err := loadCSV()
	if err != nil {
		return err
	}
	for _, record := range records {
		if _, err := x.Exec("INSERT INTO test (id, f1, f2, f3) VALUES (?,?,?,?)", record[0], record[1], record[2], record[3]); err != nil {
			return err
		}
	}
	return nil
}

func prepareInsert() error {
	x, err := openTestDB()
	if err != nil {
		return err
	}
	defer x.Close()
	records, err := loadCSV()
	if err != nil {
		return err
	}
	insertStmt, err := x.Prepare("INSERT INTO test (id, f1, f2, f3) VALUES (?,?,?,?)")
	if err != nil {
		return err
	}
	for _, record := range records {
		if _, err := insertStmt.Exec(record[0], record[1], record[2], record[3]); err != nil {
			return err
		}
	}
	return nil
}

func txInsert() error {
	x, err := openTestDB()
	if err != nil {
		return err
	}
	defer x.Close()
	tx, err := x.Begin()
	if err != nil {
		return err
	}
	records, err := loadCSV()
	if err != nil {
		return err
	}
	for _, record := range records {
		if _, err := tx.Exec("INSERT INTO test (id, f1, f2, f3) VALUES (?,?,?,?)", record[0], record[1], record[2], record[3]); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func txPrepareInsert() error {
	x, err := openTestDB()
	if err != nil {
		return err
	}
	defer x.Close()
	records, err := loadCSV()
	if err != nil {
		return err
	}
	tx, err := x.Begin()
	if err != nil {
		return err
	}
	insertStmt, err := tx.Prepare("INSERT INTO test (id, f1, f2, f3) VALUES (?,?,?,?)")
	if err != nil {
		return err
	}
	for _, record := range records {
		if _, err := insertStmt.Exec(record[0], record[1], record[2], record[3]); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func bulkInsert() error {
	x, err := openTestDB()
	if err != nil {
		return err
	}
	defer x.Close()
	records, err := loadCSV()
	if err != nil {
		return err
	}
	tx, err := x.Begin()
	if err != nil {
		return err
	}
	buf := bytes.NewBuffer([]byte("INSERT INTO test (id, f2, f2, f3) VALUES "))
	for i := 0; i < 249; i++ {
		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString("(?,?,?,?)")
	}
	bulkInsertStmt, err := tx.Prepare(buf.String())
	if err != nil {
		return err
	}
	var values []interface{}
	for _, record := range records {
		values = append(values, record[0])
		values = append(values, record[1])
		values = append(values, record[2])
		values = append(values, record[3])
		if len(values) == 996 {
			if _, err = bulkInsertStmt.Exec(values...); err != nil {
				return err
			}
			values = values[0:0]
		}
	}
	if len(values) > 0 {
		buf := bytes.NewBuffer([]byte("INSERT INTO test (id, f2, f2, f3) VALUES "))
		for i := 0; i < len(values)/4; i++ {
			if i > 0 {
				buf.WriteString(",")
			}
			buf.WriteString("(?,?,?,?)")
		}
		if _, err = tx.Exec(buf.String(), values...); err != nil {
			return err
		}
	}
	return tx.Commit()
}

const createStmt = `CREATE TABLE test (
	    id INTEGER,
	    f1 TEXT,
		f2 INTEGER,
		f3 REAL,
    PRIMARY KEY (id)
);
`

func importCSV() error {
	sqlite := exec.Command("sqlite3")
	sqlite.Stdout = os.Stdout
	w, err := sqlite.StdinPipe()
	if err != nil {
		return err
	}
	err = sqlite.Start()
	if err != nil {
		return err
	}
	_, err = w.Write([]byte(createStmt))
	if err != nil {
		return err
	}
	_, err = w.Write([]byte(".separator ,\n"))
	if err != nil {
		return err
	}
	_, err = w.Write([]byte(".import testdata.csv test\n"))
	if err != nil {
		return err
	}
	_, err = w.Write([]byte("SELECT count(*) FROM test;\n"))
	if err != nil {
		return err
	}
	w.Write([]byte(".quit\n"))
	return sqlite.Wait()
}

func gen(count int) error {
	f, err := os.Create("testdata.csv")
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	for i := 0; i < count; i++ {
		if err := w.Write(randomRecord(i)); err != nil {
			return err
		}
	}
	w.Flush()
	return nil
}

func loadCSV() ([][]string, error) {
	f, err := os.Open("testdata.csv")
	if err != nil {
		return nil, err
	}
	r := csv.NewReader(f)
	return r.ReadAll()
}

func randomRecord(i int) []string {
	return []string{strconv.Itoa(i), RandomString(20), strconv.Itoa(rand.Int()), strconv.FormatFloat(rand.Float64(), 'e', -1, 64)}
}

func RandomString(strlen int) string {
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	result := make([]byte, strlen)
	for i := 0; i < strlen; i++ {
		result[i] = chars[rand.Intn(len(chars))]
	}
	return string(result)
}
