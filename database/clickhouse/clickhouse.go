package clickhouse

import (
	"bufio"
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	"github.com/mattes/migrate"
	"github.com/mattes/migrate/database"
)

var DefaultMigrationsTable = "schema_migrations"

var ErrNilConfig = fmt.Errorf("no config")

type Config struct {
	DatabaseName    string
	MigrationsTable string
}

func init() {
	database.Register("clickhouse", &ClickHouse{})
}

func WithInstance(conn *sql.DB, config *Config) (database.Driver, error) {
	if config == nil {
		return nil, ErrNilConfig
	}

	if err := conn.Ping(); err != nil {
		return nil, err
	}

	ch := &ClickHouse{
		conn:   conn,
		config: config,
	}

	if err := ch.init(); err != nil {
		return nil, err
	}

	return ch, nil
}

type ClickHouse struct {
	conn   *sql.DB
	config *Config
}

func (ch *ClickHouse) Open(dsn string) (database.Driver, error) {
	purl, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}
	q := migrate.FilterCustomQuery(purl)
	q.Scheme = "tcp"
	conn, err := sql.Open("clickhouse", q.String())
	if err != nil {
		return nil, err
	}

	ch = &ClickHouse{
		conn: conn,
		config: &Config{
			MigrationsTable: purl.Query().Get("x-migrations-table"),
			DatabaseName:    purl.Query().Get("database"),
		},
	}

	if err := ch.init(); err != nil {
		return nil, err
	}

	return ch, nil
}

func (ch *ClickHouse) init() error {
	if len(ch.config.DatabaseName) == 0 {
		if err := ch.conn.QueryRow("SELECT currentDatabase()").Scan(&ch.config.DatabaseName); err != nil {
			return err
		}
	}

	if len(ch.config.MigrationsTable) == 0 {
		ch.config.MigrationsTable = DefaultMigrationsTable
	}

	return ch.ensureVersionTable()
}

func (ch *ClickHouse) Run(r io.Reader) error {
	statements, err := getSQLStatements(r)
	if err != nil {
		return err
	}
	for _, migration := range statements {
		if _, err := ch.conn.Exec(migration); err != nil {
			return database.Error{OrigErr: err, Err: "migration failed", Query: []byte(migration)}
		}
	}
	return nil
}

const sqlCmdPrefix = "-- +migrate "

// Checks the line to see if the line has a statement-ending semicolon
// or if the line contains a double-dash comment.
func endsWithSemicolon(line string) bool {
	prev := ""
	scanner := bufio.NewScanner(strings.NewReader(line))
	scanner.Split(bufio.ScanWords)

	for scanner.Scan() {
		word := scanner.Text()
		if strings.HasPrefix(word, "--") {
			break
		}
		prev = word
	}

	return strings.HasSuffix(prev, ";")
}

// Split the given sql script into individual statements.
//
// The base case is to simply split on semicolons, as these
// naturally terminate a statement.
//
// However, more complex cases like pl/pgsql can have semicolons
// within a statement. For these cases, we provide the explicit annotations
// 'StatementBegin' and 'StatementEnd' to allow the script to
// tell us to ignore semicolons.
func getSQLStatements(r io.Reader) (stmts []string, err error) {
	var (
		buf              bytes.Buffer
		statementEnded   bool
		ignoreSemicolons bool
		scanner          = bufio.NewScanner(r)
	)

	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, sqlCmdPrefix) {
			cmd := strings.TrimSpace(line[len(sqlCmdPrefix):])
			switch cmd {
			case "StatementBegin":
				ignoreSemicolons = true
			case "StatementEnd":
				statementEnded = (ignoreSemicolons == true)
				ignoreSemicolons = false
			}
			continue
		}

		if _, err := buf.WriteString(line + "\n"); err != nil {
			return nil, err
		}

		// Wrap up the two supported cases: 1) basic with semicolon; 2) psql statement
		// Lines that end with semicolon that are in a statement block
		// do not conclude statement.
		if (!ignoreSemicolons && endsWithSemicolon(line)) || statementEnded {
			statementEnded = false
			stmts = append(stmts, buf.String())
			buf.Reset()
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	// diagnose likely migration script errors
	if ignoreSemicolons {
		return nil, fmt.Errorf("WARNING: saw 'StatementBegin' with no matching 'StatementEnd'")
	}

	if bufferRemaining := strings.TrimSpace(buf.String()); len(bufferRemaining) > 0 {
		return nil, fmt.Errorf("WARNING: Unexpected unfinished SQL query: %s. Missing a semicolon?\n", bufferRemaining)
	}

	return stmts, nil
}

func (ch *ClickHouse) Version() (int, bool, error) {
	var (
		version int
		dirty   uint8
		query   = "SELECT version, dirty FROM `" + ch.config.MigrationsTable + "` ORDER BY sequence DESC LIMIT 1"
	)
	if err := ch.conn.QueryRow(query).Scan(&version, &dirty); err != nil {
		if err == sql.ErrNoRows {
			return database.NilVersion, false, nil
		}
		return 0, false, &database.Error{OrigErr: err, Query: []byte(query)}
	}
	return version, dirty == 1, nil
}

func (ch *ClickHouse) SetVersion(version int, dirty bool) error {
	var (
		bool = func(v bool) uint8 {
			if v {
				return 1
			}
			return 0
		}
		tx, err = ch.conn.Begin()
	)
	if err != nil {
		return err
	}

	query := "INSERT INTO " + ch.config.MigrationsTable + " (version, dirty, sequence) VALUES (?, ?, ?)"
	if _, err := tx.Exec(query, version, bool(dirty), time.Now().UnixNano()); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}

	return tx.Commit()
}

func (ch *ClickHouse) ensureVersionTable() error {
	var (
		table string
		query = "SHOW TABLES FROM " + ch.config.DatabaseName + " LIKE '" + ch.config.MigrationsTable + "'"
	)
	// check if migration table exists
	if err := ch.conn.QueryRow(query).Scan(&table); err != nil {
		if err != sql.ErrNoRows {
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
	} else {
		return nil
	}
	// if not, create the empty migration table
	query = `
		CREATE TABLE ` + ch.config.MigrationsTable + ` (
			version    UInt32, 
			dirty      UInt8,
			sequence   UInt64
		) Engine=TinyLog
	`
	if _, err := ch.conn.Exec(query); err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	return nil
}

func (ch *ClickHouse) Drop() error {
	var (
		query       = "SHOW TABLES FROM " + ch.config.DatabaseName
		tables, err = ch.conn.Query(query)
	)
	if err != nil {
		return &database.Error{OrigErr: err, Query: []byte(query)}
	}
	defer tables.Close()
	for tables.Next() {
		var table string
		if err := tables.Scan(&table); err != nil {
			return err
		}

		query = "DROP TABLE IF EXISTS " + ch.config.DatabaseName + "." + table

		if _, err := ch.conn.Exec(query); err != nil {
			return &database.Error{OrigErr: err, Query: []byte(query)}
		}
	}
	return ch.ensureVersionTable()
}

func (ch *ClickHouse) Lock() error   { return nil }
func (ch *ClickHouse) Unlock() error { return nil }
func (ch *ClickHouse) Close() error  { return ch.conn.Close() }
