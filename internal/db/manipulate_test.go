package db

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDataManipulator_FindEmailPatternByDomain(t *testing.T) {
	ctx := context.Background()
	connectService := NewMysqlConnector(ctx, 5, 2)
	db, err := connectService.Connect(
		"xxxxx",
		"xxxxxxxx",
		"xxxx:3306",
		"xxx")
	defer connectService.Disconnect()

	assert.Nil(t, err)
	assert.NotNil(t, db)

	dataService := NewDataService(ctx, db)
	emails, err := dataService.FindEmailPatternByDomain("google.com")
	assert.Nil(t, err)
	assert.Equal(t, len(emails), 1)

	emails, err = dataService.FindEmailPatternByDomain("asdfasfasdf21")
	assert.Equal(t, NOTFOUNDERR, err)
	assert.Nil(t, emails)
}

func TestDataManipulator_UpdateDomainToOld(t *testing.T) {
	ctx := context.Background()
	connectService := NewMysqlConnector(ctx, 5, 2)
	db, err := connectService.Connect(
		"xxxxx",
		"xxxxxxxx",
		"xxxx:3306",
		"xxx")
	defer connectService.Disconnect()

	assert.Nil(t, err)
	assert.NotNil(t, db)

	dataService := NewDataService(ctx, db)
	rowAffected, err := dataService.UpdateDomainToOld("google.com")
	assert.Nil(t, err)
	assert.Equal(t, int64(1), rowAffected)

	_, err = dataService.FindEmailPatternByDomain("google.com")
	assert.Equal(t, NOTFOUNDERR, err)

}