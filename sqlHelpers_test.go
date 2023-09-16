package main

import (
	"database/sql"
	"errors"
	"github.com/DATA-DOG/go-sqlmock"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

func TestOpenDbConnect(t *testing.T) {
	t.Run("Success connect", func(t *testing.T) {
		config := Config{
			dekanatDbDriverName:        "sqlite3",
			primaryDekanatDbDSN:        os.TempDir() + "/test-sqlite.db",
			primaryDekanatPingAttempts: 1,
		}

		db, err := openDbConnect(&config)

		assert.NotNil(t, db)
		assert.NoError(t, err)
	})

	t.Run("First ping error, then success connect", func(t *testing.T) {
		config := Config{
			dekanatDbDriverName:          "firebirdsql",
			primaryDekanatDbDSN:          "USER:PASSOWORD@HOST/DATABASE",
			primaryDekanatPingAttempts:   3,
			primaryDekanatPingDelay:      time.Nanosecond * 10,
			primaryDekanatReconnectDelay: time.Nanosecond * 100,
		}

		go func() {
			time.Sleep(time.Nanosecond * 2)
			config.dekanatDbDriverName = "sqlite3"
			config.primaryDekanatDbDSN = os.TempDir() + "/test-sqlite.db"
		}()
		db, err := openDbConnect(&config)

		assert.NotNil(t, db)
		assert.NoError(t, err)
	})

	t.Run("Error, then success connect", func(t *testing.T) {
		config := Config{
			dekanatDbDriverName:        "firebirdsql",
			primaryDekanatDbDSN:        "USER:PASSOWORD@HOST/DATABASE",
			primaryDekanatPingAttempts: 1,
		}

		db, err := openDbConnect(&config)

		assert.Nil(t, db)
		assert.Error(t, err)
	})

	t.Run("Wrong sql driver connect", func(t *testing.T) {
		config := Config{
			dekanatDbDriverName:        "not-exist",
			primaryDekanatDbDSN:        os.TempDir() + "/test-sqlite.db",
			primaryDekanatPingAttempts: 1,
		}

		db, err := openDbConnect(&config)

		assert.Nil(t, db)
		assert.Error(t, err)
	})
}

func TestConnectionPool(t *testing.T) {
	t.Run("Success open pool", func(t *testing.T) {
		config := Config{
			dekanatDbDriverName:        "firebirdsql",
			primaryDekanatDbDSN:        "USER:PASSOWORD@HOST/DATABASE",
			primaryDekanatPingAttempts: 0,
		}

		pool, err := createConnectionPool(&config)

		assert.Len(t, pool, ConnectionPoolSize)
		for _, c := range pool {
			assert.NotNil(t, c)
		}
		assert.NoError(t, err)
	})

	t.Run("Error open pool", func(t *testing.T) {
		config := Config{
			dekanatDbDriverName:        "firebirdsql",
			primaryDekanatDbDSN:        "USER:PASSOWORD@HOST/DATABASE",
			primaryDekanatPingAttempts: 1,
		}

		pool, err := createConnectionPool(&config)

		assert.Len(t, pool, ConnectionPoolSize)
		for _, c := range pool {
			assert.Nil(t, c)
		}
		assert.Error(t, err)
	})

	t.Run("Close pool", func(t *testing.T) {
		dbMock := [ConnectionPoolSize]sqlmock.Sqlmock{}
		pool := [ConnectionPoolSize]*sql.DB{}

		for i := 0; i < ConnectionPoolSize; i++ {
			pool[i], dbMock[i], _ = sqlmock.New()
			dbMock[i].ExpectClose()
		}

		closeConnectionPool(pool)

		for i := 0; i < ConnectionPoolSize; i++ {
			assert.NoError(t, dbMock[i].ExpectationsWereMet())
		}
	})
}

func TestQueryRowsInTransaction(t *testing.T) {
	query := "SELECT 1 FROM T_PRJURN"

	t.Run("connect error", func(t *testing.T) {
		db, dbMock, _ := sqlmock.New()

		dbMock.ExpectBegin().WillReturnError(errors.New(ConnectError))
		dbMock.ExpectBegin()

		dbMock.ExpectQuery(query).WillReturnRows(
			sqlmock.NewRows([]string{"1"}).AddRow(1),
		)

		startTime := time.Now()
		_, rows, err := queryRowsInTransaction(db, query)
		executedTime := time.Since(startTime)

		value := 0

		assert.NoError(t, err)
		assert.True(t, rows.Next())
		assert.NoError(t, rows.Scan(&value))
		assert.Equal(t, 1, value)
		assert.GreaterOrEqual(t, executedTime.Milliseconds(), int64(250))
		assert.NoError(t, dbMock.ExpectationsWereMet())
	})

	t.Run("all time connect error", func(t *testing.T) {
		db, dbMock, _ := sqlmock.New()

		dbMock.ExpectBegin().WillReturnError(errors.New(ConnectError))
		dbMock.ExpectBegin().WillReturnError(errors.New(ConnectError))
		dbMock.ExpectBegin().WillReturnError(errors.New(ConnectError))

		dbMock.ExpectQuery(query).WillReturnRows(
			sqlmock.NewRows([]string{"1"}).AddRow(1),
		)

		startTime := time.Now()
		_, _, err := queryRowsInTransaction(db, query)
		executedTime := time.Since(startTime)

		assert.Error(t, err)
		assert.Equal(t, ConnectError, err.Error())
		assert.GreaterOrEqual(t, executedTime.Milliseconds(), int64(250)*3)
	})
}
