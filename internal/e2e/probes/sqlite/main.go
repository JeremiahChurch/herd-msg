// Command sqlite checks a private home-directory database with CGO disabled.
package main

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"
)

func main() {
	home, err := os.UserHomeDir()
	check(err)
	dir, err := os.MkdirTemp(home, ".herd-msg-sqlite-")
	check(err)
	defer os.RemoveAll(dir)
	db, err := sql.Open("sqlite", filepath.Join(dir, "probe.db"))
	check(err)
	defer db.Close()
	db.SetMaxOpenConns(1)
	var mode, version string
	check(db.QueryRow("PRAGMA journal_mode=WAL").Scan(&mode))
	check(db.QueryRow("SELECT sqlite_version()").Scan(&version))
	if mode != "wal" {
		panic("WAL was not enabled")
	}
	_, err = db.Exec("CREATE TABLE probe (id INTEGER PRIMARY KEY, body TEXT NOT NULL)")
	check(err)
	tx, err := db.Begin()
	check(err)
	_, err = tx.Exec("INSERT INTO probe VALUES (1, 'committed')")
	check(err)
	check(tx.Commit())
	reader, err := sql.Open("sqlite", filepath.Join(dir, "probe.db"))
	check(err)
	defer reader.Close()
	var body string
	check(reader.QueryRow("SELECT body FROM probe WHERE id=1").Scan(&body))
	if body != "committed" {
		panic("separate connection did not read committed row")
	}
	_, err = os.Stat(filepath.Join(dir, "probe.db-wal"))
	check(err)
	fmt.Printf("sqlite=%s journal_mode=%s committed_row=%s wal_file=present\n", version, mode, body)
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}
