package main

import (
	"database/sql"
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

	t.Run("First pint error, then success connect", func(t *testing.T) {
		config := Config{
			dekanatDbDriverName:        "firebirdsql",
			primaryDekanatDbDSN:        "USER:PASSOWORD@HOST/DATABASE",
			primaryDekanatPingAttempts: 2,
		}

		go func() {
			time.Sleep(time.Nanosecond * 50)
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
