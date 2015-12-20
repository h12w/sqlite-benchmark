package main

import (
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
)

const createStmt = `CREATE TABLE %s (
	    id INTEGER,
	    f1 TEXT,
		f2 INTEGER,
		f3 REAL,
    PRIMARY KEY (id)
);
`

func main() {
	x, err := openTestDB()
	if err != nil {
		panic(err)
	}
	tx, err := x.Begin()
	if err != nil {
		panic(err)
	}
	ins1, err := tx.Prepare("INSERT INTO table1 (id, f1, f2, f3) VALUES (?,?,?,?)")
	if err != nil {
		panic(err)
	}
	ins2, err := tx.Prepare("INSERT INTO table2 (id, f1, f2, f3) VALUES (?,?,?,?)")
	if err != nil {
		panic(err)
	}
	if _, err := ins1.Exec(0, "a", 0, 0.0); err != nil {
		panic(err)
	}
	if _, err := ins2.Exec(3, "d", 3, 0.3); err != nil {
		panic(err)
	}
	if _, err := ins1.Exec(1, "b", 1, 0.1); err != nil {
		panic(err)
	}
	if _, err := ins2.Exec(4, "e", 4, 0.4); err != nil {
		panic(err)
	}
	if _, err := ins1.Exec(2, "c", 2, 0.2); err != nil {
		panic(err)
	}
	if err := tx.Commit(); err != nil {
		panic(err)
	}
	{
		row := x.QueryRow("SELECT count(*) FROM table1")
		var cnt int
		row.Scan(&cnt)
		fmt.Println(cnt)
	}
	{
		row := x.QueryRow("SELECT count(*) FROM table2")
		var cnt int
		row.Scan(&cnt)
		fmt.Println(cnt)
	}

}

func openTestDB() (*sql.DB, error) {
	x, err := sql.Open("sqlite3", "file:test?mode=memory&cache=shared")
	if err != nil {
		return nil, err
	}
	if _, err := x.Exec(fmt.Sprintf(createStmt, "table1")); err != nil {
		return nil, err
	}
	if _, err := x.Exec(fmt.Sprintf(createStmt, "table2")); err != nil {
		return nil, err
	}
	return x, nil
}
