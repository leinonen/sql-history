package parser

import (
	"regexp"
	"strings"
)

type Config struct {
	TrackUser  bool
	UserSource string
}

type Table struct {
	Name        string
	SchemaName  string
	FullName    string
	Schema      string
	Columns     []Column
	ForeignKeys []ForeignKey
}

type Column struct {
	Name     string
	DataType string
	Options  string
}

type ForeignKey struct {
	ColumnName       string
	ReferencedTable  string
	ReferencedColumn string
	OnDelete         string
	OnUpdate         string
}

func ParseCreateTables(sqlContent string) ([]Table, error) {
	var tables []Table

	tableRegex := regexp.MustCompile(`(?i)CREATE\s+TABLE\s+([^\s(]+)\s*\((.*?)\);`)

	content := strings.ReplaceAll(sqlContent, "\n", " ")
	content = strings.ReplaceAll(content, "\t", " ")
	content = regexp.MustCompile(`\s+`).ReplaceAllString(content, " ")

	createStart := regexp.MustCompile(`(?i)CREATE\s+TABLE\s+`)
	starts := createStart.FindAllStringIndex(content, -1)

	for _, start := range starts {
		tableEnd := findTableEnd(content, start[0])
		if tableEnd > start[0] {
			tableSQL := content[start[0]:tableEnd]
			match := tableRegex.FindStringSubmatch(tableSQL)
			if len(match) >= 3 {
				fullTableName := strings.Trim(match[1], "`\"[]")
				columnsStr := match[2]

				schemaName, tableName := parseTableName(fullTableName)

				table := Table{
					Name:       tableName,
					SchemaName: schemaName,
					FullName:   fullTableName,
					Schema:     tableSQL,
				}

				columns, foreignKeys, err := ParseColumns(columnsStr)
				if err != nil {
					continue
				}
				table.Columns = columns
				table.ForeignKeys = foreignKeys

				tables = append(tables, table)
			}
		}
	}

	return tables, nil
}

func findTableEnd(content string, start int) int {
	parenCount := 0
	inQuotes := false
	var quoteChar rune

	for i := start; i < len(content); i++ {
		r := rune(content[i])

		switch r {
		case '\'', '"':
			if !inQuotes {
				inQuotes = true
				quoteChar = r
			} else if r == quoteChar {
				inQuotes = false
			}
		case '(':
			if !inQuotes {
				parenCount++
			}
		case ')':
			if !inQuotes {
				parenCount--
			}
		case ';':
			if !inQuotes && parenCount == 0 {
				return i + 1
			}
		}
	}

	return len(content)
}

func parseTableName(fullTableName string) (string, string) {
	parts := strings.Split(fullTableName, ".")
	if len(parts) == 2 {
		return strings.Trim(parts[0], "`\"[]"), strings.Trim(parts[1], "`\"[]")
	}
	return "", strings.Trim(fullTableName, "`\"[]")
}

func ParseColumns(columnsStr string) ([]Column, []ForeignKey, error) {
	var columns []Column
	var foreignKeys []ForeignKey

	lines := splitColumns(columnsStr)

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		upperLine := strings.ToUpper(line)
		if strings.HasPrefix(upperLine, "PRIMARY KEY") ||
			strings.HasPrefix(upperLine, "CONSTRAINT") ||
			strings.HasPrefix(upperLine, "INDEX") ||
			strings.HasPrefix(upperLine, "KEY") {
			continue
		}

		if strings.HasPrefix(upperLine, "FOREIGN KEY") {
			fk := parseForeignKeyConstraint(line)
			if fk.ColumnName != "" {
				foreignKeys = append(foreignKeys, fk)
			}
			continue
		}

		colName, dataType, options := parseColumnDefinition(line)
		if colName != "" && dataType != "" {
			columns = append(columns, Column{
				Name:     colName,
				DataType: dataType,
				Options:  options,
			})

			fk := extractInlineForeignKey(colName, options)
			if fk.ColumnName != "" {
				foreignKeys = append(foreignKeys, fk)
			}
		}
	}

	return columns, foreignKeys, nil
}

func splitColumns(columnsStr string) []string {
	var result []string
	var current strings.Builder
	parenCount := 0
	inQuotes := false
	var quoteChar rune

	for _, r := range columnsStr {
		switch r {
		case '\'', '"':
			if !inQuotes {
				inQuotes = true
				quoteChar = r
			} else if r == quoteChar {
				inQuotes = false
			}
			current.WriteRune(r)
		case '(':
			if !inQuotes {
				parenCount++
			}
			current.WriteRune(r)
		case ')':
			if !inQuotes {
				parenCount--
			}
			current.WriteRune(r)
		case ',':
			if !inQuotes && parenCount == 0 {
				result = append(result, current.String())
				current.Reset()
			} else {
				current.WriteRune(r)
			}
		default:
			current.WriteRune(r)
		}
	}

	if current.Len() > 0 {
		result = append(result, current.String())
	}

	return result
}

func parseColumnDefinition(line string) (string, string, string) {
	line = strings.TrimSpace(line)

	spaceIdx := strings.Index(line, " ")
	if spaceIdx == -1 {
		return "", "", ""
	}

	colName := strings.Trim(line[:spaceIdx], "`\"[]")
	rest := strings.TrimSpace(line[spaceIdx+1:])

	dataType, options := extractDataType(rest)

	return colName, dataType, options
}

func extractDataType(rest string) (string, string) {
	parenCount := 0
	inQuotes := false
	var quoteChar rune
	dataTypeEnd := -1

	for i, r := range rest {
		if r == ' ' && !inQuotes && parenCount == 0 {
			dataTypeEnd = i
			break
		}

		switch r {
		case '\'', '"':
			if !inQuotes {
				inQuotes = true
				quoteChar = r
			} else if r == quoteChar {
				inQuotes = false
			}
		case '(':
			if !inQuotes {
				parenCount++
			}
		case ')':
			if !inQuotes {
				parenCount--
			}
		}
	}

	if dataTypeEnd == -1 {
		return strings.TrimSpace(rest), ""
	}

	dataType := strings.TrimSpace(rest[:dataTypeEnd])
	options := strings.TrimSpace(rest[dataTypeEnd:])

	return dataType, options
}

func parseForeignKeyConstraint(line string) ForeignKey {
	fkRegex := regexp.MustCompile(`(?i)FOREIGN\s+KEY\s*\(\s*([^)]+)\s*\)\s+REFERENCES\s+([^\s(]+)\s*\(\s*([^)]+)\s*\)(?:\s+ON\s+DELETE\s+((?:SET\s+NULL|SET\s+DEFAULT|RESTRICT|CASCADE|NO\s+ACTION|\w+)))?(?:\s+ON\s+UPDATE\s+((?:SET\s+NULL|SET\s+DEFAULT|RESTRICT|CASCADE|NO\s+ACTION|\w+)))?`)

	match := fkRegex.FindStringSubmatch(line)
	if len(match) >= 4 {
		columnName := strings.TrimSpace(strings.Trim(match[1], "`\"[]"))
		referencedTable := strings.TrimSpace(strings.Trim(match[2], "`\"[]"))
		referencedColumn := strings.TrimSpace(strings.Trim(match[3], "`\"[]"))

		onDelete := ""
		if len(match) > 4 && match[4] != "" {
			onDelete = strings.ToUpper(match[4])
		}

		onUpdate := ""
		if len(match) > 5 && match[5] != "" {
			onUpdate = strings.ToUpper(match[5])
		}

		return ForeignKey{
			ColumnName:       columnName,
			ReferencedTable:  referencedTable,
			ReferencedColumn: referencedColumn,
			OnDelete:         onDelete,
			OnUpdate:         onUpdate,
		}
	}

	return ForeignKey{}
}

func extractInlineForeignKey(columnName, options string) ForeignKey {
	referencesRegex := regexp.MustCompile(`(?i)REFERENCES\s+([^\s(]+)\s*\(\s*([^)]+)\s*\)(?:\s+ON\s+DELETE\s+((?:SET\s+NULL|SET\s+DEFAULT|RESTRICT|CASCADE|NO\s+ACTION|\w+)))?(?:\s+ON\s+UPDATE\s+((?:SET\s+NULL|SET\s+DEFAULT|RESTRICT|CASCADE|NO\s+ACTION|\w+)))?`)

	match := referencesRegex.FindStringSubmatch(options)
	if len(match) >= 3 {
		referencedTable := strings.TrimSpace(strings.Trim(match[1], "`\"[]"))
		referencedColumn := strings.TrimSpace(strings.Trim(match[2], "`\"[]"))

		onDelete := ""
		if len(match) > 3 && match[3] != "" {
			onDelete = strings.ToUpper(match[3])
		}

		onUpdate := ""
		if len(match) > 4 && match[4] != "" {
			onUpdate = strings.ToUpper(match[4])
		}

		return ForeignKey{
			ColumnName:       columnName,
			ReferencedTable:  referencedTable,
			ReferencedColumn: referencedColumn,
			OnDelete:         onDelete,
			OnUpdate:         onUpdate,
		}
	}

	return ForeignKey{}
}
