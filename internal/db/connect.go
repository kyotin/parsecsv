package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"time"
)

var MAKECONNECTIONERR = errors.New("can't make connection")

const (
	BackoffInitDefault     = 500 * time.Millisecond
	BackoffFactorDefault   = 2.0
	MaxBackoffTimesDefault = 3
)

type ConnectService interface {
	Connect(username string, password string, uri string, dbName string) (*sql.DB, error)
	Disconnect() error
}

type MysqlConnector struct {
	Ctx               context.Context
	BackoffInit       time.Duration
	BackoffFactor     float64
	MaxBackoffTimes   uint8
	MaxOpenConnection int
	MaxIdleConnection int
	backoffTimes      uint8
	db                *sql.DB
}

func NewMysqlConnector(ctx context.Context, maxOpenConnection int, maxIdleConnection int) ConnectService {
	return &MysqlConnector{
		Ctx:               ctx,
		BackoffInit:       BackoffInitDefault,
		BackoffFactor:     BackoffFactorDefault,
		MaxBackoffTimes:   MaxBackoffTimesDefault,
		MaxOpenConnection: maxOpenConnection,
		MaxIdleConnection: maxIdleConnection,
		backoffTimes: 0,
	}
}

func (connector *MysqlConnector) Connect(username string, password string, uri string, dbName string) (*sql.DB, error) {
	if connector.backoffTimes == connector.MaxBackoffTimes {
		return nil, MAKECONNECTIONERR
	}

	db, _ := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", username, password, uri, dbName))
	db.SetMaxOpenConns(connector.MaxOpenConnection)
	db.SetMaxIdleConns(connector.MaxIdleConnection)
	err := db.PingContext(connector.Ctx)
	if err != nil {
		waitTime := connector.getWaitTime()
		log.Printf("Make connection fail, wait %d milisecond", waitTime)
		<-time.After(waitTime)
		return connector.Connect(username, password, uri, dbName)
	}
	connector.db = db
	return db, err
}

func (connector *MysqlConnector) Disconnect() error {
	return connector.db.Close()
}

func (connector *MysqlConnector) getWaitTime() time.Duration {
	var waitTime time.Duration
	if connector.backoffTimes == 0 {
		waitTime = connector.BackoffInit
	} else {
		waitTime = connector.BackoffInit * time.Duration(connector.BackoffFactor*float64(connector.backoffTimes))
	}

	connector.backoffTimes += 1
	return waitTime
}
