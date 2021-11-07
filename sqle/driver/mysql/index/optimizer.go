package index

import (
	"strconv"

	"github.com/actiontech/sqle/sqle/driver/mysql/context"
	"github.com/actiontech/sqle/sqle/driver/mysql/executor"

	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// Optimizer store the SQL parts and context. Context store enough info of an database instance.
type Optimizer struct {
	l   *logrus.Entry
	cfg *Config
	db  *executor.Executor
	ctx *context.Context

	ce *ColumnExtractor
}

// Config defines the config of the optimizer.
type Config struct {
	// RangeMaxRow define the threshold of the range select. If
	// range selected rows returned more than this value, we will
	// not give advice.
	//
	// ref: https://dev.mysql.com/doc/refman/5.7/en/range-optimization.html
	RangeMaxRow int

	// CalcCardinalityMaxRow define the threshold of selectivity calculation.
	// If show table status Rows return more than this value, we will not calculate
	// the selectivity.
	CalcSelectivityMaxRow int64
}

func NewOptimizer(l *logrus.Entry, ctx *context.Context, db *executor.Db, cfg *Config) *Optimizer {
	return &Optimizer{
		l:   l,
		ctx: ctx,
	}
}

// Optimize optimize two things:
// 	1. the index of table:
//		We will give advice to add index when the columns part of the SQL
//		string is not indexed.
// 	2. the SQL string:
//		We will give advice to change SQL string when the columns part of
//		the SQL string is indexed, but MySQL optimizer can not use the index by explain plan.
//
// We parse the sql string to get the ast.StmtNode. Then find the known
// columns in the ast tree. Such as:
// 	1. the where condition
// 	2. the group by condition
// 	3. the order by condition
// 	4. the select columns
// 	5. the join columns
// 	6. the join condition
//
// Then we will get the table meta lazily during the process. Such as:
// 	1. the column selectivity
// 	2. the already indexed columns
// 	3. the table storage engine and MySQL version.
func (o *Optimizer) Optmize(sql string) (advice string, err error) {
	nodes, _, err := parser.New().PerfectParse(sql, "", "")
	if err != nil {
		return "", errors.Wrap(err, "optimizer parse sql error")
	}

	if len(nodes) != 1 {
		o.l.Warningf("the SQL(%s) is not a single statement, we only support single statement now", sql)
		return "", nil
	}

	ce := &ColumnExtractor{}
	nodes[0].Accept(ce)

	o.ce = ce

	_, _ = o.optmizeHook(sql)
	return "", nil
}

// getSelectivity get the selectivity of the column.
// s equal to 0 and err is nil means that every column's selectivity equals to 0.
// So the index optimization is not accurate.
func (o *Optimizer) getSelectivity(table, column string) (s float64, err error) {
	s, exist := o.ctx.GetSelectivity(table, column)
	if exist {
		return s, nil
	}

	ts, err := o.db.ShowTableStatus(table)
	if err != nil {
		return 0, errors.Wrap(err, "optimizer get table status error")
	}
	if !ts.Rows.Valid {
		return 0, errors.Errorf("column Rows is invalid in show table status")
	}

	// Rows euqal to 0 means the table is empty. Means that every
	// column's selectivity is 1.
	if ts.Rows.Int64 == 0 {
		return 0, nil
	}

	if int64(o.cfg.CalcSelectivityMaxRow) < ts.Rows.Int64 {
		return 0, nil
	}

	rows, err := o.db.Db.Query("select count(distinct `%s`) as cardinality from `%s`", column, table)
	if err != nil {
		return 0, errors.Wrap(err, "optimizer get column cardinality error")
	}
	cardinality, err := strconv.ParseInt(rows[0]["cardinality"].String, 10, 64)
	if err != nil {
		return 0, errors.Wrap(err, "optimizer parse column cardinality error")
	}

	s = float64(cardinality) / float64(ts.Rows.Int64)
	o.ctx.SetSelectivity(table, column, s)

	return
}

type Advice struct {
	// alter is the alter sql string. Such as:
	// ALTER TABLE `test` ADD INDEX `idx_name` (`name`)
	alter string

	// reason is the reason why we give the advice.
	reason string
}

type column struct {
	ci *model.ColumnInfo
	cl ColumnLocation
}

type ColumnLocation string

const (
	ColumnLocationSelect  ColumnLocation = "select"
	ColumnLocationWhere   ColumnLocation = "where"
	ColumnLocationGroupBy ColumnLocation = "group by"
	ColumnLocationOrderBy ColumnLocation = "order by"
	ColumnLocationJoin    ColumnLocation = "join"
)

// ColumnExtractor implements ast.Visitor interface in TiDB parser.
// It is used to get column name in a table.
type ColumnExtractor struct {
	cols map[ast.ColumnName]*column
}

func (c *ColumnExtractor) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	return in, false
}

func (c *ColumnExtractor) Leave(in ast.Node) (out ast.Node, ok bool) {
	return in, true
}
