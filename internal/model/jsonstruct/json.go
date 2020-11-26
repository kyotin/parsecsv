package jsonstruct

//json
type _Source struct {
	PersonName                        string   `json:"person_name"`
	PersonFirstNameUnanalyzed         string   `json:"person_first_name_unanalyzed"`
	PersonLastNameUnanalyzed          string   `json:"person_last_name_unanalyzed"`
	PersonNameUnanalyzedDowncase      string   `json:"person_name_unanalyzed_downcase"`
	PersonEmailStatusCd               string   `json:"person_email_status_cd"`
	PersonExtrapolatedEmailConfidence float32  `json:"person_extrapolated_email_confidence"`
	PersonExtrapolatedEmail           string   `json:"person_extrapolated_email"`
	PersonEmail                       string   `json:"person_email"`
	PersonLinkedinUrl                 string   `json:"person_linkedin_url"`
	PersonPhone                       string   `json:"person_phone"`
	PersonLocalCountry                string   `json:"person_location_country"`
	SanitizedOrganizationName         string   `json:"sanitized_organization_name_unanalyzed"`
	OrganizationName                  string   `json:"organization_name"`
	OrganizationLinkedinNumericalUrls []string `json:"organization_linkedin_numerical_urls"`
	Origin                            string   `json:"origin"`
}

func (s _Source) IsNotValid() bool {
	return (s.PersonPhone == "" && s.PersonEmail == "") ||
		(s.PersonEmail == "" && s.PersonLinkedinUrl == "") ||
		(s.PersonPhone == "" && s.PersonLinkedinUrl == "") ||
		(s.PersonEmail == "" && s.OrganizationName == "")
}

type Record struct {
	Index  string  `json:"_index"`
	Type   string  `json:"_type"`
	Source _Source `json:"_source"`
}

type ContactDB struct {
	PersonName                        string   `json:"person_name"`
	PersonFirstNameUnanalyzed         string   `json:"person_first_name_unanalyzed"`
	PersonLastNameUnanalyzed          string   `json:"person_last_name_unanalyzed"`
	PersonNameUnanalyzedDowncase      string   `json:"person_name_unanalyzed_downcase"`
	PersonEmailStatusCd               string   `json:"person_email_status_cd"`
	PersonExtrapolatedEmailConfidence float32  `json:"person_extrapolated_email_confidence"`
	PersonExtrapolatedEmail           string   `json:"person_extrapolated_email"`
	PersonEmail                       string   `json:"person_email"`
	PersonLinkedinUrl                 string   `json:"person_linkedin_url"`
	PersonPhone                       string   `json:"person_phone"`
	PersonLocalCountry                string   `json:"person_location_country"`
	SanitizedOrganizationName         string   `json:"sanitized_organization_name_unanalyzed"`
	OrganizationName                  string   `json:"organization_name"`
	OrganizationLinkedinNumericalUrls []string `json:"organization_linkedin_numerical_urls"`
	Origin                            string   `json:"origin"`
}

type RecordWithCompany struct {
	Index  string  `json:"_index"`
	Type   string  `json:"_type"`
	Source CompanyDB `json:"_source"`
}

type CompanyDB struct {
	OrganizationName string `json:"organization_name"`
	SanitizedOrganizationNameUnanalyzed string `json:"sanitized_organization_name_unanalyzed"`
	OrganizationFoundedYear int `json:"organization_founded_year"`
	OrganizationNumCurrentEmployees int `json:"organization_num_current_employees"`
	OrganizationAlexaRanking float64 `json:"organization_alexa_ranking"`
	OrganizationRelevantKeywords []string `json:"organization_relevant_keywords"`
	OrganizationIndustries []string`json:"organization_industries"`
	OrganizationLinkedinSpecialties string `json:"organization_linkedin_specialties"`
	OrganizationWebsiteUrl string `json:"organization_website_url"`
	OrganizationTwitterUrl string`json:"organization_twitter_url"`
	OrganizationLinkedNumericalUrls []string `json:"organization_linkedin_numerical_urls"`
	OrganizationPhone string `json:"organization_phone"`
	OrganizationAllPossibleDomains []string `json:"organization_all_possible_domains"`
	OrganizationCurrentTechinologies []string `json:"organization_current_technologies"`
	OrganizationHqLocationCity string `json:"organization_hq_location_city"`
	OrganizationHqLocationCityWithStateOrCountry string `json:"organization_hq_location_city_with_state_or_country"`
	OrganizationHqLocationState string `json:"organization_hq_location_state"`
	OrganizationHqLocationCountry string `json:"organization_hq_location_country"`
}