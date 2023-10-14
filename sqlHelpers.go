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
    ID,
    ID_ZANCG AS CUSTOM_GROUP_LESSON_ID,
    NUM_PREDM, DATEZAN, NUM_VARZAN, HALF,
    (case FSTATUS when 0 then 1 else 0 end) as isDeleted 
FROM T_PRJURN `

const ScoreSelect = `SELECT ID, ID_OBJ AS STUDENT_ID,
	XI_2 AS LESSON_ID, XI_4 as LESSON_PART,
	ID_ZANCG AS CUSTOM_GROUP_LESSON_ID,
	ID_T_PD_CMS AS DISCIPLINE_ID, XI_5 as SEMESTER, 
	COALESCE(XR_1, 0) AS SCORE,
	(case COALESCE(XS10_4, 'NULL') when 'NULL' then 0 else 1 end) AS IS_ABSENT,
	REGDATE, 
    ( case XS10_5
        when 'Так' then case COALESCE(XR_1, XS10_4, 'NULL') when 'NULL' then 1 else 0 end
    	else 1 end ) AS IS_DELETED
FROM T_EV_9 `

// ScoreSelectOrderBy uses `UNIQUE INDEX UNQ1_T_EV_9 ON T_EV_9 (ID_OBJ, XI_2, XI_4)` to have sequential student-score in result
const ScoreSelectOrderBy = ` ORDER BY ID_OBJ, XI_2, XI_4 ASC`

const ConnectError = "_parse_connect_response() protocol error"

func queryRowsInTransaction(db *sql.DB, query string, args ...any) (tx *sql.Tx, rows *sql.Rows, err error) {
	for i := 0; i < 3; i++ {
		tx, err = db.BeginTx(context.Background(), &sql.TxOptions{
			Isolation: DefaultTransactionIsolation,
			ReadOnly:  true,
		})

		if err == nil || err.Error() != ConnectError {
			break
		}

		time.Sleep(time.Millisecond * 250)
	}

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
	for i := uint8(0); i <= config.primaryDekanatPingAttempts; i++ {
		if i%3 == 0 {
			primaryDekanatDb, err = sql.Open(config.dekanatDbDriverName, config.primaryDekanatDbDSN)
			if err != nil {
				return nil, err
			}
		}
		if config.primaryDekanatPingAttempts == 0 {
			return
		}

		ctx, cancel = context.WithTimeout(context.Background(), time.Millisecond*400)
		err = primaryDekanatDb.PingContext(ctx)
		cancel()
		if err == nil {
			return
		}
		if i%3 != 2 {
			time.Sleep(config.primaryDekanatPingDelay)
		} else {
			time.Sleep(config.primaryDekanatReconnectDelay)
		}
	}
	return nil, err
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

func getTodayString() string {
	now := time.Now()
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.Local)
	return today.Format(FirebirdTimeFormat)
}
