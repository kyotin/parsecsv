package db

import (
	"context"
	"database/sql"
	"errors"
	"parsecsv/internal/model/emailpattern"
	"parsecsv/internal/model/jsonstruct"
	"strings"
)

var NOTFOUNDERR = errors.New("not found")
var DOMAINEXISTEDERR = errors.New("domain name existed")
var COMMITEDERR = errors.New("can not commit transaction")

type DataService interface {
	FindEmailPatternByDomain(domain string) ([]*emailpattern.EmailPattern, error)
	UpdateDomainToOld(domain string) (int64, error)
	InsertNewEmailPattern(emailPattern emailpattern.EmailPattern) (int64, error)
	DeleteDomain(domain string) (int64, error)
	GetMaxID() (int64, error)
	FindEmailPatternByIDRange(start int64, end int64) ([]*emailpattern.EmailPattern, error)
	InsertNewCompany(company *jsonstruct.CompanyDB) (int64, error)
}

type dataManipulator struct {
	ctx context.Context
	db  *sql.DB
}

func NewDataService(ctx context.Context, db *sql.DB) DataService {
	return &dataManipulator{
		ctx: ctx,
		db:  db,
	}
}

func (manipulator *dataManipulator) FindEmailPatternByIDRange(start int64, end int64) ([]*emailpattern.EmailPattern, error) {
	var emails []*emailpattern.EmailPattern
	// Execute the query
	rows, err := manipulator.
		db.
		Query(""+
			"SELECT id, score1, pattern1, score2, pattern2, score3, pattern3, domain_name, entry "+
			"FROM email_pattern "+
			"WHERE id >= ? AND id <= ?",
			start,
			end,
		)

	if err != nil {
		return nil, err
	}

	for rows.Next() {
		emailPattern := &emailpattern.EmailPattern{}
		err := rows.Scan(&emailPattern.ID,
			&emailPattern.Score1,
			&emailPattern.Pattern1,
			&emailPattern.Score2,
			&emailPattern.Pattern2,
			&emailPattern.Score3,
			&emailPattern.Pattern3,
			&emailPattern.DomainName,
			&emailPattern.Entry)

		if err == nil {
			emails = append(emails, emailPattern)
		}
	}

	if len(emails) == 0 {
		return nil, NOTFOUNDERR
	}

	return emails, nil
}

func (manipulator *dataManipulator) GetMaxID() (int64, error) {
	result, err := manipulator.db.QueryContext(manipulator.ctx, "SELECT max(id) FROM email_pattern")
	if err != nil {
		return 0, err
	}

	var maxID int64

	if !result.Next() {
		return 0, err
	}

	if err = result.Scan(&maxID); err == nil {
		return maxID, nil
	} else {
		return 0, err
	}
}

func (manipulator *dataManipulator) FindEmailPatternByDomain(domain string) ([]*emailpattern.EmailPattern, error) {
	var emails []*emailpattern.EmailPattern
	// Execute the query
	rows, err := manipulator.
		db.
		Query(""+
			"SELECT id, score1, pattern1, score2, pattern2, score3, pattern3, domain_name, entry "+
			"FROM email_pattern "+
			"WHERE domain_name = ?",
			domain,
		)

	if err != nil {
		return nil, err
	}

	for rows.Next() {
		emailPattern := &emailpattern.EmailPattern{}
		err := rows.Scan(&emailPattern.ID,
			&emailPattern.Score1,
			&emailPattern.Pattern1,
			&emailPattern.Score2,
			&emailPattern.Pattern2,
			&emailPattern.Score3,
			&emailPattern.Pattern3,
			&emailPattern.DomainName,
			&emailPattern.Entry)

		if err == nil {
			emails = append(emails, emailPattern)
		}
	}

	if len(emails) == 0 {
		return nil, NOTFOUNDERR
	}

	return emails, nil
}

func (manipulator *dataManipulator) UpdateDomainToOld(domain string) (int64, error) {
	txOption := &sql.TxOptions{
		Isolation: sql.LevelSerializable,
		ReadOnly:  false,
	}
	tx, err := manipulator.db.BeginTx(manipulator.ctx, txOption)
	if err != nil {
		return 0, err
	}

	newDomain := "old_" + domain
	result, err := tx.ExecContext(manipulator.ctx, "UPDATE email_pattern SET domain_name = ? WHERE domain_name = ?", newDomain, domain)
	if err != nil {
		return 0, err
	}

	if err = tx.Commit(); err != nil {
		return 0, COMMITEDERR
	}

	return result.RowsAffected()
}

func (manipulator *dataManipulator) InsertNewEmailPattern(emailPattern emailpattern.EmailPattern) (int64, error) {
	txOption := &sql.TxOptions{
		Isolation: sql.LevelSerializable,
		ReadOnly:  false,
	}
	tx, err := manipulator.db.BeginTx(manipulator.ctx, txOption)
	if err != nil {
		return 0, err
	}

	rowsExisted, err := tx.QueryContext(manipulator.ctx, "SELECT domain_name FROM email_pattern WHERE domain_name = ?", emailPattern.DomainName)
	if err != nil {
		return 0, err
	}

	if rowsExisted.Next() {
		if err = tx.Commit(); err != nil {
			return 0, COMMITEDERR
		}

		return 0, DOMAINEXISTEDERR
	}

	result, err := tx.ExecContext(manipulator.ctx,
		"INSERT INTO email_pattern(score1, pattern1, score2, pattern2, score3, pattern3, domain_name, entry) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
		emailPattern.Score1,
		emailPattern.Pattern1,
		emailPattern.Score2,
		emailPattern.Pattern2,
		emailPattern.Score3,
		emailPattern.Pattern3,
		emailPattern.DomainName,
		emailPattern.Entry)

	if err != nil {
		return 0, err
	}

	if err = tx.Commit(); err != nil {
		return 0, COMMITEDERR
	}

	return result.RowsAffected()
}

func (manipulator *dataManipulator) InsertNewCompany(company *jsonstruct.CompanyDB) (int64, error) {
	txOption := &sql.TxOptions{
		Isolation: sql.LevelSerializable,
		ReadOnly:  false,
	}
	tx, err := manipulator.db.BeginTx(manipulator.ctx, txOption)
	if err != nil {
		return 0, err
	}

	result, err := tx.ExecContext(manipulator.ctx,
		"INSERT INTO company_apollo(" +
		"organization_name," +
		"sanitized_organization_name_unanalyzed," +
		"organization_founded_year," +
		"organization_num_current_employees," +
		"organization_alexa_ranking," +
		"organization_relevant_keywords," +
		"organization_industries," +
		"organization_linkedin_specialties," +
		"organization_website_url," +
		"organization_twitter_url," +
		"organization_linkedin_numerical_urls," +
		"organization_phone," +
		"organization_all_possible_domains," +
		"organization_current_technologies," +
		"organization_hq_location_city," +
		"organization_hq_location_city_with_state_or_country," +
		"organization_hq_location_state," +
		"organization_hq_location_country" +
		") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
		company.OrganizationName,
		company.SanitizedOrganizationNameUnanalyzed,
		company.OrganizationFoundedYear,
		company.OrganizationNumCurrentEmployees,
		company.OrganizationAlexaRanking,
		strings.Join(company.OrganizationRelevantKeywords,"|"),
		strings.Join(company.OrganizationIndustries,"|"),
		company.OrganizationLinkedinSpecialties,
		company.OrganizationWebsiteUrl,
		company.OrganizationTwitterUrl,
		strings.Join(company.OrganizationLinkedNumericalUrls, "|"),
		company.OrganizationPhone,
		strings.Join(company.OrganizationAllPossibleDomains, "|"),
		strings.Join(company.OrganizationCurrentTechinologies, "|"),
		company.OrganizationHqLocationCity,
		company.OrganizationHqLocationCityWithStateOrCountry,
		company.OrganizationHqLocationState,
		company.OrganizationHqLocationCountry)

	if err != nil {
		return 0, err
	}

	if err = tx.Commit(); err != nil {
		return 0, COMMITEDERR
	}

	return result.RowsAffected()
}

func (manipulator *dataManipulator) DeleteDomain(domain string) (int64, error) {
	txOption := &sql.TxOptions{
		Isolation: sql.LevelSerializable,
		ReadOnly:  false,
	}
	tx, err := manipulator.db.BeginTx(manipulator.ctx, txOption)
	if err != nil {
		return 0, err
	}

	result, err := tx.ExecContext(manipulator.ctx,
		"DELETE FROM email_pattern WHERE domain_name = ?",
		domain)

	if err != nil {
		return 0, err
	}

	if err = tx.Commit(); err != nil {
		return 0, COMMITEDERR
	}

	return result.RowsAffected()
}
