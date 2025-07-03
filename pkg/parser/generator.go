package parser

import (
	"fmt"
	"strings"
)

func GenerateHistoryTable(table Table) string {
	var sb strings.Builder

	historyTableName := GetHistoryTableName(table)

	sb.WriteString(fmt.Sprintf("CREATE TABLE %s (\n", historyTableName))

	for _, col := range table.Columns {
		sb.WriteString(fmt.Sprintf("    %s %s", col.Name, col.DataType))
		if col.Options != "" && !strings.Contains(strings.ToUpper(col.Options), "PRIMARY KEY") &&
			!strings.Contains(strings.ToUpper(col.Options), "AUTO_INCREMENT") {
			cleanOptions := strings.ReplaceAll(col.Options, "PRIMARY KEY", "")
			cleanOptions = strings.ReplaceAll(cleanOptions, "AUTO_INCREMENT", "")
			cleanOptions = strings.TrimSpace(cleanOptions)
			if cleanOptions != "" {
				sb.WriteString(" " + cleanOptions)
			}
		}
		sb.WriteString(",\n")
	}

	sb.WriteString("    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\n")
	sb.WriteString("    valid_to TIMESTAMP NULL,\n")
	sb.WriteString("    operation CHAR(1) NOT NULL CHECK (operation IN ('I', 'U', 'D'))\n")
	sb.WriteString(");\n\n")

	indexPrefix := getIndexPrefix(table)
	sb.WriteString(fmt.Sprintf("CREATE INDEX idx_%s_history_valid_from ON %s (valid_from);\n", indexPrefix, historyTableName))
	sb.WriteString(fmt.Sprintf("CREATE INDEX idx_%s_history_valid_to ON %s (valid_to);\n", indexPrefix, historyTableName))

	return sb.String()
}

func GenerateTriggers(table Table) string {
	var sb strings.Builder

	historyTableName := GetHistoryTableName(table)
	originalTableName := GetOriginalTableName(table)

	columnNames := make([]string, len(table.Columns))
	newValues := make([]string, len(table.Columns))

	for i, col := range table.Columns {
		columnNames[i] = col.Name
		newValues[i] = "NEW." + col.Name
	}

	columnsStr := strings.Join(columnNames, ", ")
	newValuesStr := strings.Join(newValues, ", ")

	sb.WriteString(fmt.Sprintf("-- Insert trigger for %s\n", originalTableName))
	sb.WriteString(fmt.Sprintf("CREATE OR REPLACE FUNCTION %s_insert_history() RETURNS TRIGGER AS $$\n", GetFunctionPrefix(table)))
	sb.WriteString("BEGIN\n")
	sb.WriteString(fmt.Sprintf("    INSERT INTO %s (%s, valid_from, operation)\n", historyTableName, columnsStr))
	sb.WriteString(fmt.Sprintf("    VALUES (%s, CURRENT_TIMESTAMP, 'I');\n", newValuesStr))
	sb.WriteString("    RETURN NEW;\n")
	sb.WriteString("END;\n")
	sb.WriteString("$$ LANGUAGE plpgsql;\n\n")

	sb.WriteString(fmt.Sprintf("CREATE TRIGGER %s_insert_trigger\n", GetFunctionPrefix(table)))
	sb.WriteString(fmt.Sprintf("    AFTER INSERT ON %s\n", originalTableName))
	sb.WriteString("    FOR EACH ROW\n")
	sb.WriteString(fmt.Sprintf("    EXECUTE FUNCTION %s_insert_history();\n\n", GetFunctionPrefix(table)))

	sb.WriteString(fmt.Sprintf("-- Update trigger for %s\n", originalTableName))
	sb.WriteString(fmt.Sprintf("CREATE OR REPLACE FUNCTION %s_update_history() RETURNS TRIGGER AS $$\n", GetFunctionPrefix(table)))
	sb.WriteString("BEGIN\n")
	sb.WriteString(fmt.Sprintf("    UPDATE %s SET valid_to = CURRENT_TIMESTAMP\n", historyTableName))
	sb.WriteString("    WHERE valid_to IS NULL")

	primaryKeys := GetPrimaryKeyColumns(table)
	if len(primaryKeys) > 0 {
		sb.WriteString(" AND ")
		conditions := make([]string, len(primaryKeys))
		for i, pk := range primaryKeys {
			conditions[i] = fmt.Sprintf("%s = OLD.%s", pk, pk)
		}
		sb.WriteString(strings.Join(conditions, " AND "))
	}
	sb.WriteString(";\n")

	sb.WriteString(fmt.Sprintf("    INSERT INTO %s (%s, valid_from, operation)\n", historyTableName, columnsStr))
	sb.WriteString(fmt.Sprintf("    VALUES (%s, CURRENT_TIMESTAMP, 'U');\n", newValuesStr))
	sb.WriteString("    RETURN NEW;\n")
	sb.WriteString("END;\n")
	sb.WriteString("$$ LANGUAGE plpgsql;\n\n")

	sb.WriteString(fmt.Sprintf("CREATE TRIGGER %s_update_trigger\n", GetFunctionPrefix(table)))
	sb.WriteString(fmt.Sprintf("    AFTER UPDATE ON %s\n", originalTableName))
	sb.WriteString("    FOR EACH ROW\n")
	sb.WriteString(fmt.Sprintf("    EXECUTE FUNCTION %s_update_history();\n\n", GetFunctionPrefix(table)))

	sb.WriteString(fmt.Sprintf("-- Delete trigger for %s\n", originalTableName))
	sb.WriteString(fmt.Sprintf("CREATE OR REPLACE FUNCTION %s_delete_history() RETURNS TRIGGER AS $$\n", GetFunctionPrefix(table)))
	sb.WriteString("BEGIN\n")
	sb.WriteString(fmt.Sprintf("    UPDATE %s SET valid_to = CURRENT_TIMESTAMP, operation = 'D'\n", historyTableName))
	sb.WriteString("    WHERE valid_to IS NULL")

	if len(primaryKeys) > 0 {
		sb.WriteString(" AND ")
		conditions := make([]string, len(primaryKeys))
		for i, pk := range primaryKeys {
			conditions[i] = fmt.Sprintf("%s = OLD.%s", pk, pk)
		}
		sb.WriteString(strings.Join(conditions, " AND "))
	}
	sb.WriteString(";\n")
	sb.WriteString("    RETURN OLD;\n")
	sb.WriteString("END;\n")
	sb.WriteString("$$ LANGUAGE plpgsql;\n\n")

	sb.WriteString(fmt.Sprintf("CREATE TRIGGER %s_delete_trigger\n", GetFunctionPrefix(table)))
	sb.WriteString(fmt.Sprintf("    BEFORE DELETE ON %s\n", originalTableName))
	sb.WriteString("    FOR EACH ROW\n")
	sb.WriteString(fmt.Sprintf("    EXECUTE FUNCTION %s_delete_history();\n\n", GetFunctionPrefix(table)))

	return sb.String()
}

func GetPrimaryKeyColumns(table Table) []string {
	var primaryKeys []string
	for _, col := range table.Columns {
		if strings.Contains(strings.ToUpper(col.Options), "PRIMARY KEY") {
			primaryKeys = append(primaryKeys, col.Name)
		}
	}

	if len(primaryKeys) == 0 && len(table.Columns) > 0 {
		primaryKeys = append(primaryKeys, table.Columns[0].Name)
	}

	return primaryKeys
}

func GeneratePointInTimeQuery(table Table) string {
	return ""
}

func GetHistoryTableName(table Table) string {
	if table.SchemaName != "" {
		return fmt.Sprintf("%s.%s_history", table.SchemaName, table.Name)
	}
	return fmt.Sprintf("%s_history", table.Name)
}

func GetOriginalTableName(table Table) string {
	if table.SchemaName != "" {
		return fmt.Sprintf("%s.%s", table.SchemaName, table.Name)
	}
	return table.Name
}

func GetFunctionPrefix(table Table) string {
	if table.SchemaName != "" {
		return fmt.Sprintf("%s_%s", table.SchemaName, table.Name)
	}
	return table.Name
}

func getIndexPrefix(table Table) string {
	if table.SchemaName != "" {
		return fmt.Sprintf("%s_%s", table.SchemaName, table.Name)
	}
	return table.Name
}

func GenerateHistorySQL(tables []Table) (string, error) {
	var sb strings.Builder

	sb.WriteString("-- Generated History Tables and Triggers\n")
	sb.WriteString("-- This file contains history tables and triggers for temporal data tracking\n\n")

	for i, table := range tables {
		if i > 0 {
			sb.WriteString("\n" + strings.Repeat("-", 80) + "\n\n")
		}

		sb.WriteString(fmt.Sprintf("-- History table and triggers for: %s\n\n", GetOriginalTableName(table)))

		historyTable := GenerateHistoryTable(table)
		sb.WriteString(historyTable)
		sb.WriteString("\n")

		triggers := GenerateTriggers(table)
		sb.WriteString(triggers)

	}


	return sb.String(), nil
}
