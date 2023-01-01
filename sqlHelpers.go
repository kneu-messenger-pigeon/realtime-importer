package main

import (
	"context"
	"database/sql"
	"strings"
	"sync"
	"time"
)

const ConnectionPoolSize = 4
const DefaultTransactionIsolation = sql.LevelReadCommitted
const FirebirdTimeFormat = "2006-01-02 15:04:05"

const LessonsSelect = `SELECT 
    ID, NUM_PREDM, DATEZAN, NUM_VARZAN, HALF,
    (case FSTATUS when 0 then 1 else 0 end) as isDeleted 
FROM T_PRJURN `

const ScoreSelect = `SELECT ID, ID_OBJ AS STUDENT_ID,
	XI_2 AS LESSON_ID, XI_4 as LESSON_PART,
	ID_T_PD_CMS AS DISCIPLINE_ID, XI_5 as SEMESTER, 
	COALESCE(XR_1, 0) AS SCORE, (case COALESCE(XS10_4, 'NULL') when 'NULL' then 0 else 1 end) AS IS_ABSENT,
	REGDATE, 
    ( case XS10_5
        when 'Так' then case COALESCE(XR_1, 'NULL') when 'NULL' then 1 else 0 end
    	else 1 end ) AS IS_DELETED
FROM T_EV_9 `

func queryRowsInTransaction(db *sql.DB, query string, args ...any) (tx *sql.Tx, rows *sql.Rows, err error) {
	tx, err = db.BeginTx(context.Background(), &sql.TxOptions{
		Isolation: DefaultTransactionIsolation,
		ReadOnly:  true,
	})
	if err == nil {
		rows, err = tx.Query(query, args...)
	}
	return
}

func closeRowsAndTransaction(rows *sql.Rows, tx *sql.Tx) {
	if rows != nil {
		_ = rows.Close()
	}
	if tx != nil {
		_ = tx.Rollback()
	}
}

func openDbConnect(config *Config) (primaryDekanatDb *sql.DB, err error) {
	var ctx context.Context
	var cancel context.CancelFunc
	for i := uint8(0); i < config.primaryDekanatPingAttempts; i++ {
		primaryDekanatDb, err = sql.Open(config.dekanatDbDriverName, config.primaryDekanatDbDSN)
		if err != nil {
			return nil, err
		}

		ctx, cancel = context.WithTimeout(context.Background(), time.Millisecond*400)
		err = primaryDekanatDb.PingContext(ctx)
		cancel()
		if err == nil {
			return
		}
		primaryDekanatDb.Close()
		time.Sleep(time.Microsecond * 100)
	}

	return nil, nil
}

func createConnectionPool(config *Config) (pool [ConnectionPoolSize]*sql.DB, err error) {
	var nextErr error
	var wg sync.WaitGroup

	openConnection := func(index int) {
		pool[index], nextErr = openDbConnect(config)
		if err == nil && nextErr != nil {
			err = nextErr
		}
		wg.Done()
	}
	wg.Add(ConnectionPoolSize)
	for i := 0; i < ConnectionPoolSize; i++ {
		go openConnection(i)
	}
	wg.Wait()

	return
}

func closeConnectionPool(pool [ConnectionPoolSize]*sql.DB) {
	for i := 0; i < ConnectionPoolSize; i++ {
		if pool[i] != nil {
			_ = pool[i].Close()
		}
	}
}

func extractInPlaceHolder(query string, count int) string {
	return strings.Replace(query, "?", "?"+strings.Repeat(",?", count-1), 1)
}
