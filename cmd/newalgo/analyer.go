package main

import (
	"errors"
	"fmt"
	"strings"
)

type Analyzer struct {
	Pattern string
	Domain  Domain

	email     string
	firstname string
	lastname  string

	Err error
}

func (out *Analyzer) AnalysePattern() {
	if (out.firstname == "" && out.lastname == "") || out.email == "" {
		out.Err = errors.New("not enough data for analyzing")
		return
	}

	out.firstname = strings.ReplaceAll(out.firstname, "'", "")
	out.firstname = strings.ReplaceAll(out.firstname, "`", "")
	out.lastname = strings.ReplaceAll(out.lastname, "'", "")
	out.lastname = strings.ReplaceAll(out.lastname, "`", "")

	parts := strings.Split(out.email, "@")
	if len(parts) != 2 {
		out.Err = errors.New(fmt.Sprintf("look like the email is wrong %s", out.email))
		return
	}

	out.Domain = Domain(parts[1])

	buildingPattern := parts[0]
	if out.firstname == "" && out.lastname == buildingPattern {
		buildingPattern = "Last"
	} else if out.lastname == "" && out.firstname == buildingPattern {
		buildingPattern = "First"
	} else if out.lastname != "" && out.firstname != "" {
		firstNameIdx := strings.Index(buildingPattern, out.firstname)
		lastNameIdx := strings.Index(buildingPattern, out.lastname)

		if firstNameIdx == -1 && lastNameIdx == -1 {
			if len(buildingPattern) == 2 &&
				(buildingPattern[0] == out.firstname[0] || buildingPattern[0] == out.lastname[0]) &&
				(buildingPattern[1] == out.firstname[0] || buildingPattern[1] == out.lastname[0]) {
				buildingPattern = "Initial"
			} else {
				out.Err = errors.New(fmt.Sprintf("Can't aware pattern from %s, %s, %s", out.email, out.firstname, out.lastname))
				return
			}
		} else if firstNameIdx != -1 && lastNameIdx != -1 {
			if lastNameIdx < len(out.firstname) && len(out.firstname)-lastNameIdx+1 == 2 && len(out.firstname) >= 2 && len(buildingPattern) >= 2 && buildingPattern[0] == out.firstname[0] && buildingPattern[1] == out.firstname[1] {
				buildingPattern = "TwofirstletteroffirstLast"
			} else {
				buildingPattern = strings.ReplaceAll(buildingPattern, out.firstname, "First")
				buildingPattern = strings.ReplaceAll(buildingPattern, out.lastname, "Last")
			}
		} else if firstNameIdx != -1 && lastNameIdx == -1 {
			if firstNameIdx == 0 {
				remainingLength := len(parts[0]) - len(out.firstname)
				switch remainingLength {
				case 0:
					buildingPattern = "First"
				case 1:
					buildingPattern = strings.ReplaceAll(buildingPattern, out.firstname, "First")
					if buildingPattern[len("First")] == out.lastname[0] {
						buildingPattern = "FirstL"
					} else {
						out.Err = errors.New(fmt.Sprintf("Can't aware pattern from %s, %s, %s", out.email, out.firstname, out.lastname))
						return
					}
				case 2:
					buildingPattern = strings.ReplaceAll(buildingPattern, out.firstname, "First")
					if buildingPattern[len("First")] == out.lastname[0] && buildingPattern[len("First")+1] == out.lastname[1] {
						buildingPattern = "FirstTwofirstletteroflast"
					} else if buildingPattern[len("First")+1] == out.lastname[0] {
						tmp := strings.ReplaceAll(buildingPattern, "First", "")
						tmp = strings.ReplaceAll(tmp, string(out.lastname[0]), "L")
						buildingPattern = "First" + tmp
					} else {
						out.Err = errors.New(fmt.Sprintf("Can't aware pattern from %s, %s, %s", out.email, out.firstname, out.lastname))
						return
					}
				default:
					out.Err = errors.New(fmt.Sprintf("Can't aware pattern from %s, %s, %s", out.email, out.firstname, out.lastname))
					return
				}
			}

			if firstNameIdx > 0 {
				remainingLength := len(parts[0]) - len(out.firstname)
				switch remainingLength {
				case 1:
					buildingPattern = strings.ReplaceAll(buildingPattern, out.firstname, "First")
					if buildingPattern[0] == out.lastname[0] {
						buildingPattern = "LFirst"
					} else {
						out.Err = errors.New(fmt.Sprintf("Can't aware pattern from %s, %s, %s", out.email, out.firstname, out.lastname))
						return
					}
				case 2:
					buildingPattern = strings.ReplaceAll(buildingPattern, out.firstname, "First")
					if buildingPattern[0] == out.lastname[0] && buildingPattern[1] == out.lastname[1] {
						buildingPattern = "TwofirstletteroflastFirst"
					} else if buildingPattern[0] == out.lastname[0] {
						tmp := strings.ReplaceAll(buildingPattern, "First", "")
						tmp = strings.ReplaceAll(tmp, string(out.lastname[0]), "L")
						buildingPattern = tmp + "First"
					} else {
						out.Err = errors.New(fmt.Sprintf("Can't aware pattern from %s, %s, %s", out.email, out.firstname, out.lastname))
						return
					}
				default:
					out.Err = errors.New(fmt.Sprintf("Can't aware pattern from %s, %s, %s", out.email, out.firstname, out.lastname))
					return
				}
			}
		} else if firstNameIdx == -1 && lastNameIdx != -1 {
			if lastNameIdx == 0 {
				remainingLength := len(parts[0]) - len(out.lastname)
				switch remainingLength {
				case 0:
					buildingPattern = "Last"
				case 1:
					buildingPattern = strings.ReplaceAll(buildingPattern, out.lastname, "Last")
					if buildingPattern[len("Last")] == out.firstname[0] {
						buildingPattern = "LastF"
					} else {
						out.Err = errors.New(fmt.Sprintf("Can't aware pattern from %s, %s, %s", out.email, out.firstname, out.lastname))
						return
					}
				case 2:
					buildingPattern = strings.ReplaceAll(buildingPattern, out.lastname, "Last")
					if buildingPattern[len("Last")] == out.firstname[0] && buildingPattern[len("Last")+1] == out.firstname[1] {
						buildingPattern = "LastTwofirstletteroffirst"
					} else if buildingPattern[len("Last")+1] == out.firstname[0] {
						tmp := strings.ReplaceAll(buildingPattern, "Last", "")
						tmp = strings.ReplaceAll(tmp, string(out.firstname[0]), "F")
						buildingPattern = "Last" + tmp
					} else {
						out.Err = errors.New(fmt.Sprintf("Can't aware pattern from %s, %s, %s", out.email, out.firstname, out.lastname))
						return
					}
				default:
					out.Err = errors.New(fmt.Sprintf("Can't aware pattern from %s, %s, %s", out.email, out.firstname, out.lastname))
					return
				}
			}

			if lastNameIdx > 0 {
				remainingLength := len(parts[0]) - len(out.lastname)
				switch remainingLength {
				case 1:
					buildingPattern = strings.ReplaceAll(buildingPattern, out.lastname, "Last")
					if buildingPattern[0] == out.firstname[0] {
						buildingPattern = "FLast"
					} else {
						out.Err = errors.New(fmt.Sprintf("Can't aware pattern from %s, %s, %s", out.email, out.firstname, out.lastname))
						return
					}
				case 2:
					buildingPattern = strings.ReplaceAll(buildingPattern, out.lastname, "Last")
					if buildingPattern[0] == out.firstname[0] && buildingPattern[1] == out.firstname[1] {
						buildingPattern = "TwofirstletteroffirstLast"
					} else if buildingPattern[0] == out.firstname[0] {
						tmp := strings.ReplaceAll(buildingPattern, "Last", "")
						tmp = strings.ReplaceAll(tmp, string(out.firstname[0]), "F")
						buildingPattern = tmp + "Last"
					} else {
						out.Err = errors.New(fmt.Sprintf("Can't aware pattern from %s, %s, %s", out.email, out.firstname, out.lastname))
						return
					}
				default:
					out.Err = errors.New(fmt.Sprintf("Can't aware pattern from %s, %s, %s", out.email, out.firstname, out.lastname))
					return
				}
			}
		}
	} else {
		out.Err = errors.New(fmt.Sprintf("Can't aware pattern from %s, %s, %s", out.email, out.firstname, out.lastname))
		return
	}

	out.Pattern = buildingPattern
}
