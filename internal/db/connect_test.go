package db

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMysqlConnector_ConnectFail(t *testing.T) {
	connector := NewMysqlConnector(context.Background(), 5, 2)
	db, err := connector.Connect("abc", "xxxxx", "localhost:3600", "test")
	assert.Equal(t, MAKECONNECTIONERR, err)
	assert.Nil(t, db)
}

func TestMysqlConnector_Connect(t *testing.T) {
	connectService := NewMysqlConnector(context.Background(), 5, 2)
	db, err := connectService.Connect(
		"xxxxx",
		"xxxxxxxx",
		"xxxx:3306",
		"xxx")
	defer connectService.Disconnect()

	assert.Nil(t, err)
	assert.NotNil(t, db)
}