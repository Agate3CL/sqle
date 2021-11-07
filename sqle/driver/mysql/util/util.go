package util

import "github.com/pingcap/parser/ast"

func MergeAlterToTable(oldTable *ast.CreateTableStmt, alterTable *ast.AlterTableStmt) (*ast.CreateTableStmt, error) {
	newTable := &ast.CreateTableStmt{
		Table:       oldTable.Table,
		Cols:        oldTable.Cols,
		Constraints: oldTable.Constraints,
		Options:     oldTable.Options,
		Partition:   oldTable.Partition,
	}
	for _, spec := range getAlterTableSpecByTp(alterTable.Specs, ast.AlterTableRenameTable) {
		newTable.Table = spec.NewTable
	}
	for _, spec := range getAlterTableSpecByTp(alterTable.Specs, ast.AlterTableDropColumn) {
		colExists := false
		for i, col := range newTable.Cols {
			if col.Name.Name.L == spec.OldColumnName.Name.L {
				colExists = true
				newTable.Cols = append(newTable.Cols[:i], newTable.Cols[i+1:]...)
			}
		}
		if !colExists {
			return oldTable, nil
		}
	}
	for _, spec := range getAlterTableSpecByTp(alterTable.Specs, ast.AlterTableChangeColumn) {
		colExists := false
		for i, col := range newTable.Cols {
			if col.Name.Name.L == spec.OldColumnName.Name.L {
				colExists = true
				newTable.Cols[i] = spec.NewColumns[0]
			}
		}
		if !colExists {
			return oldTable, nil
		}
	}
	for _, spec := range getAlterTableSpecByTp(alterTable.Specs, ast.AlterTableModifyColumn) {
		colExists := false
		for i, col := range newTable.Cols {
			if col.Name.Name.L == spec.NewColumns[0].Name.Name.L {
				colExists = true
				newTable.Cols[i] = spec.NewColumns[0]
			}
		}
		if !colExists {
			return oldTable, nil
		}
	}
	for _, spec := range getAlterTableSpecByTp(alterTable.Specs, ast.AlterTableAlterColumn) {
		colExists := false
		newCol := spec.NewColumns[0]
		for _, col := range newTable.Cols {
			if col.Name.Name.L == newCol.Name.Name.L {
				colExists = true
				// alter table alter column drop default
				if newCol.Options == nil {
					for i, op := range col.Options {
						if op.Tp == ast.ColumnOptionDefaultValue {
							col.Options = append(col.Options[:i], col.Options[i+1:]...)
						}
					}
				} else {
					if HasOneInOptions(col.Options, ast.ColumnOptionDefaultValue) {
						for i, op := range col.Options {
							if op.Tp == ast.ColumnOptionDefaultValue {
								col.Options[i] = newCol.Options[0]
							}
						}
					} else {
						col.Options = append(col.Options, newCol.Options...)
					}
				}
			}
		}
		if !colExists {
			return oldTable, nil
		}
	}

	for _, spec := range getAlterTableSpecByTp(alterTable.Specs, ast.AlterTableAddColumns) {
		for _, newCol := range spec.NewColumns {
			colExist := false
			for _, col := range newTable.Cols {
				if col.Name.Name.L == newCol.Name.Name.L {
					colExist = true
				}
			}
			if colExist {
				return oldTable, nil
			}
			newTable.Cols = append(newTable.Cols, newCol)
		}
	}

	for _, spec := range getAlterTableSpecByTp(alterTable.Specs, ast.AlterTableDropPrimaryKey) {
		_ = spec
		if !hasPrimaryKey(newTable) {
			return oldTable, nil
		}
		for i, constraint := range newTable.Constraints {
			switch constraint.Tp {
			case ast.ConstraintPrimaryKey:
				newTable.Constraints = append(newTable.Constraints[:i], newTable.Constraints[i+1:]...)
			}
		}
		for _, col := range newTable.Cols {
			for i, op := range col.Options {
				switch op.Tp {
				case ast.ColumnOptionPrimaryKey:
					col.Options = append(col.Options[:i], col.Options[i+1:]...)
				}
			}
		}
	}

	for _, spec := range getAlterTableSpecByTp(alterTable.Specs, ast.AlterTableDropIndex) {
		indexName := spec.Name
		constraintExists := false
		for i, constraint := range newTable.Constraints {
			if constraint.Name == indexName {
				constraintExists = true
				newTable.Constraints = append(newTable.Constraints[:i], newTable.Constraints[i+1:]...)
			}
		}
		if !constraintExists {
			return oldTable, nil
		}
	}

	for _, spec := range getAlterTableSpecByTp(alterTable.Specs, ast.AlterTableRenameIndex) {
		oldName := spec.FromKey
		newName := spec.ToKey
		constraintExists := false
		for _, constraint := range newTable.Constraints {
			if constraint.Name == oldName.String() {
				constraintExists = true
				constraint.Name = newName.String()
			}
		}
		if !constraintExists {
			return oldTable, nil
		}
	}

	for _, spec := range getAlterTableSpecByTp(alterTable.Specs, ast.AlterTableAddConstraint) {
		switch spec.Constraint.Tp {
		case ast.ConstraintPrimaryKey:
			if hasPrimaryKey(newTable) {
				return oldTable, nil
			}
			newTable.Constraints = append(newTable.Constraints, spec.Constraint)
		default:
			constraintExists := false
			for _, constraint := range newTable.Constraints {
				if constraint.Name == spec.Constraint.Name {
					constraintExists = true
				}
			}
			if constraintExists {
				return oldTable, nil
			}
			newTable.Constraints = append(newTable.Constraints, spec.Constraint)
		}
	}
	return newTable, nil
}

func getAlterTableSpecByTp(specs []*ast.AlterTableSpec, ts ...ast.AlterTableType) []*ast.AlterTableSpec {
	s := []*ast.AlterTableSpec{}
	if specs == nil {
		return s
	}
	for _, spec := range specs {
		for _, tp := range ts {
			if spec.Tp == tp {
				s = append(s, spec)
			}
		}
	}
	return s
}

func getPrimaryKey(stmt *ast.CreateTableStmt) (map[string]struct{}, bool) {
	hasPk := false
	pkColumnsName := map[string]struct{}{}
	for _, constraint := range stmt.Constraints {
		if constraint.Tp == ast.ConstraintPrimaryKey {
			hasPk = true
			for _, col := range constraint.Keys {
				pkColumnsName[col.Column.Name.L] = struct{}{}
			}
		}
	}
	if !hasPk {
		for _, col := range stmt.Cols {
			if HasOneInOptions(col.Options, ast.ColumnOptionPrimaryKey) {
				hasPk = true
				pkColumnsName[col.Name.Name.L] = struct{}{}
			}
		}
	}
	return pkColumnsName, hasPk
}

func hasPrimaryKey(stmt *ast.CreateTableStmt) bool {
	_, hasPk := getPrimaryKey(stmt)
	return hasPk
}

func HasOneInOptions(Options []*ast.ColumnOption, opTp ...ast.ColumnOptionType) bool {
	// has one exists, return true
	for _, tp := range opTp {
		for _, op := range Options {
			if tp == op.Tp {
				return true
			}
		}
	}
	return false
}
