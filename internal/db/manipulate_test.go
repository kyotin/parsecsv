package db

import (
	"context"
	"github.com/stretchr/testify/assert"
	"parsecsv/internal/model/emailpattern"
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

func TestDataManipulator_InsertDeleteEmailPattern(t *testing.T) {
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

	emailPattern := emailpattern.EmailPattern{
		Score1:     33.33,
		Pattern1:   "First.Last",
		Score2:     33.33,
		Pattern2:   "Last.First",
		Score3:     33.33,
		Pattern3:   "Others",
		DomainName: "oto4u.vn",
		Entry:      100,
	}
	dataService := NewDataService(ctx, db)
	insertedRows, err := dataService.InsertNewEmailPattern(emailPattern)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), insertedRows)

	emails, err := dataService.FindEmailPatternByDomain("oto4u.vn")
	assert.Nil(t, err)
	assert.Equal(t, "oto4u.vn", emails[0].DomainName)
	assert.Equal(t, 33.33, emails[0].Score1)
	assert.Equal(t, "First.Last", emails[0].Pattern1)
	assert.Equal(t, 33.33, emails[0].Score2)
	assert.Equal(t, "Last.First", emails[0].Pattern2)
	assert.Equal(t, 33.33, emails[0].Score3)
	assert.Equal(t, "Others", emails[0].Pattern3)

	_, err = dataService.InsertNewEmailPattern(emailPattern)
	assert.Equal(t, DOMAINEXISTEDERR, err)

	deletedRows, err := dataService.DeleteDomain("oto4u.vn")
	assert.Nil(t, err)
	assert.Equal(t, int64(1), deletedRows)
}