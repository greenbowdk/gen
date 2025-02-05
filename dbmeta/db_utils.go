package dbmeta

import (
	"bytes"
	"fmt"
)

// PrimaryKeyCount return the number of primary keys in table
func PrimaryKeyCount(dbTable DbTableMeta) int {
	primaryKeys := 0
	for _, col := range dbTable.Columns() {
		if col.IsPrimaryKey() {
			primaryKeys++
		}
	}
	return primaryKeys
}

// PrimaryKeyNames return the list of primary key names
func PrimaryKeyNames(dbTable DbTableMeta) []string {
	primaryKeyNames := make([]string, 0)
	for _, col := range dbTable.Columns() {
		if col.IsPrimaryKey() {
			primaryKeyNames = append(primaryKeyNames, col.Name())
		}
	}
	return primaryKeyNames
}

// NonPrimaryKeyNames return the list of primary key names
func NonPrimaryKeyNames(dbTable DbTableMeta) []string {
	primaryKeyNames := make([]string, 0)
	for _, col := range dbTable.Columns() {
		if !col.IsPrimaryKey() {
			primaryKeyNames = append(primaryKeyNames, col.Name())
		}
	}
	return primaryKeyNames
}

// GenerateHardDeleteSQL generate sql for a delete
func GenerateHardDeleteSQL(dbTable DbTableMeta, namedParams bool) (string, error) {
	primaryCnt := PrimaryKeyCount(dbTable)

	if primaryCnt == 0 {
		return "", fmt.Errorf("table %s does not have a primary key, cannot generate sql", dbTable.TableName())
	}

	buf := bytes.Buffer{}
	buf.WriteString(fmt.Sprintf(`DELETE FROM "%s" where`, dbTable.TableName()))

	addedKey := 1
	for _, col := range dbTable.Columns() {
		if col.IsPrimaryKey() {
			param := fmt.Sprintf("$%d", addedKey)
			if namedParams {
				param = fmt.Sprintf("@%s_%d", col.Name(), addedKey)
			}
			buf.WriteString(fmt.Sprintf(" %s = %s", col.Name(), param))
			addedKey++

			if addedKey < primaryCnt {
				buf.WriteString(" AND")
			}
		}
	}

	return buf.String(), nil
}

// GenerateSoftDeleteSQL generate sql for a soft delete (update)
func GenerateSoftDeleteSQL(dbTable DbTableMeta, namedParams bool) (string, error) {
	primaryCnt := PrimaryKeyCount(dbTable)
	// nonPrimaryCnt := len(dbTable.Columns()) - primaryCnt

	if primaryCnt == 0 {
		return "", fmt.Errorf("table %s does not have a primary key, cannot generate sql", dbTable.TableName())
	}

	buf := bytes.Buffer{}
	buf.WriteString(fmt.Sprintf(`UPDATE "%s" set`, dbTable.TableName()))

	setCol := 1
	for _, col := range dbTable.Columns() {
		if col.Name() != "deleted_at" && col.Name() != "DeletedAt" {
			continue
		}

		if setCol != 1 {
			buf.WriteString(",")
		}

		param := fmt.Sprintf("$%d", setCol)
		if namedParams {
			param = fmt.Sprintf("@upd_%s_%d", col.Name(), setCol)
		}
		buf.WriteString(fmt.Sprintf(" %s = %s", col.Name(), param))
		setCol++
	}

	if setCol == 1 {
		return "", fmt.Errorf("table %s does not have a deleted at column, cannot generate sql", dbTable.TableName())
	}

	buf.WriteString(" WHERE")
	for _, col := range dbTable.Columns() {
		if col.IsPrimaryKey() {
			param := fmt.Sprintf("$%d", setCol)
			if namedParams {
				param = fmt.Sprintf("@%s", col.Name())
			}
			buf.WriteString(fmt.Sprintf(" %s = %s", col.Name(), param))

			setCol++
		}
	}

	return buf.String(), nil
}

// GenerateUpdateSQL generate sql for a update
func GenerateUpdateSQL(dbTable DbTableMeta, namedParams bool) (string, error) {
	primaryCnt := PrimaryKeyCount(dbTable)
	// nonPrimaryCnt := len(dbTable.Columns()) - primaryCnt

	if primaryCnt == 0 {
		return "", fmt.Errorf("table %s does not have a primary key, cannot generate sql", dbTable.TableName())
	}

	buf := bytes.Buffer{}
	buf.WriteString(fmt.Sprintf(`UPDATE "%s" SET`, dbTable.TableName()))

	setCol := 1
	for _, col := range dbTable.Columns() {
		if !col.IsPrimaryKey() {
			if setCol != 1 {
				buf.WriteString(",")
			}

			param := fmt.Sprintf("$%d", setCol)
			if namedParams {
				param = fmt.Sprintf("@%s", col.Name())
			}
			buf.WriteString(fmt.Sprintf(" %s = %s", col.Name(), param))
			setCol++
		}
	}

	buf.WriteString(" WHERE")
	addedKey := 1
	for _, col := range dbTable.Columns() {
		if col.IsPrimaryKey() {
			param := fmt.Sprintf("$%d", addedKey+setCol)
			if namedParams {
				param = fmt.Sprintf("@where_%s", col.Name())
			}
			buf.WriteString(fmt.Sprintf(" %s = %s", col.Name(), param))

			setCol++
			addedKey++

			if addedKey < primaryCnt {
				buf.WriteString(" AND")
			}
		}
	}

	return buf.String(), nil
}

// GenerateInsertSQL generate sql for a insert
func GenerateInsertSQL(dbTable DbTableMeta, namedParams bool) (string, error) {
	primaryCnt := PrimaryKeyCount(dbTable)

	if primaryCnt == 0 {
		return "", fmt.Errorf("table %s does not have a primary key, cannot generate sql", dbTable.TableName())
	}

	buf := bytes.Buffer{}
	buf.WriteString(fmt.Sprintf(`INSERT INTO "%s" (`, dbTable.TableName()))

	pastFirst := false
	for _, col := range dbTable.Columns() {
		if !col.IsAutoIncrement() {
			if pastFirst {
				buf.WriteString(", ")
			}

			buf.WriteString(fmt.Sprintf(" %s", col.Name()))
			pastFirst = true
		}
	}
	buf.WriteString(") values ( ")

	pastFirst = false
	pos := 1
	for i, col := range dbTable.Columns() {
		if !col.IsAutoIncrement() {
			if pastFirst {
				buf.WriteString(", ")
			}

			param := fmt.Sprintf("$%d", i+1)
			if namedParams {
				param = fmt.Sprintf("@%s", col.Name())
			}
			if col.IsPrimaryKey() {
				param = "default"
			}

			buf.WriteString(fmt.Sprintf("%s", param))
			pos++
			pastFirst = true
		}
	}

	buf.WriteString(" )")
	return buf.String(), nil
}

// GenerateSelectOneSQL generate sql for selecting one record
func GenerateSelectOneSQL(dbTable DbTableMeta, namedParams bool) (string, error) {
	primaryCnt := PrimaryKeyCount(dbTable)

	if primaryCnt == 0 {
		return "", fmt.Errorf("table %s does not have a primary key, cannot generate sql", dbTable.TableName())
	}

	buf := bytes.Buffer{}
	buf.WriteString(fmt.Sprintf(`SELECT * FROM "%s" WHERE`, dbTable.TableName()))

	pastFirst := false
	pos := 1
	for i, col := range dbTable.Columns() {
		if col.IsPrimaryKey() {
			if pastFirst {
				buf.WriteString(" AND ")
			}

			param := fmt.Sprintf("$%d", i+1)
			if namedParams {
				param = fmt.Sprintf("@where_%s_%d", col.Name(), i+1)
			}
			buf.WriteString(fmt.Sprintf(" %s = %s", col.Name(), param))
			pos++
			pastFirst = true
		}
	}
	return buf.String(), nil
}

// GenerateSelectMultiSQL generate sql for selecting multiple records
func GenerateSelectMultiSQL(dbTable DbTableMeta, namedParams bool) (string, error) {
	primaryCnt := PrimaryKeyCount(dbTable)

	if primaryCnt == 0 {
		return "", fmt.Errorf("table %s does not have a primary key, cannot generate sql", dbTable.TableName())
	}

	buf := bytes.Buffer{}
	buf.WriteString(fmt.Sprintf(`SELECT * FROM "%s" WHERE`, dbTable.TableName()))

	pastFirst := false
	pos := 1
	for i, col := range dbTable.Columns() {
		if col.IsPrimaryKey() {
			if pastFirst {
				buf.WriteString(" AND ")
			}

			param := fmt.Sprintf("$%d", i+1)
			if namedParams {
				param = fmt.Sprintf("@where_%s_%d", col.Name(), i+1)
			}
			buf.WriteString(fmt.Sprintf(" %s = ANY(%s::%s[])", col.Name(), param, col.ColumnType()))
			pos++
			pastFirst = true
		}
	}
	return buf.String(), nil
}

// GenerateSelectAllSQL generate sql for selecting multiple records
func GenerateSelectAllSQL(dbTable DbTableMeta) (string, error) {
	primaryCnt := PrimaryKeyCount(dbTable)

	if primaryCnt == 0 {
		return "", fmt.Errorf("table %s does not have a primary key, cannot generate sql", dbTable.TableName())
	}

	buf := bytes.Buffer{}
	buf.WriteString(fmt.Sprintf(`SELECT * FROM "%s"`, dbTable.TableName()))
	return buf.String(), nil
}
