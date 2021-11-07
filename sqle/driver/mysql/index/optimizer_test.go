package index

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/actiontech/sqle/sqle/driver/mysql/context"
	"github.com/actiontech/sqle/sqle/log"
	"github.com/stretchr/testify/assert"
)

func getUTContext() *context.Context {
	return nil
}

func TestOptimizer_Optmize(t *testing.T) {
	cases := []struct {
		setUp   func(t *testing.T, db *sql.DB, e sqlmock.Sqlmock) *Optimizer
		sql     string
		advices []*Advice
		wantErr bool
	}{
		// 1. 范围查询 2. 范围查询返回的列太多。 不会建议加索引，因为此时还不如全表扫。
		// 可以添加一个配置 DQLIndexOptimizeRangeMinRows DQL 范围查询超过多少则不生成索引建议
		//{},

		// 使用 MySQL 虚拟列来优化 Where 中带有函数的查询。
		//{
		//sql: "select * from t1 where max(c1) = xxx",
		//},
	}
	for i, tt := range cases {
		t.Run(fmt.Sprintf("case:%d", i), func(t *testing.T) {
			db, m, err := sqlmock.New()
			assert.NoError(t, err)
			o := tt.setUp(t, db, m)
			advices, err := o.Optmize(tt.sql)
			if tt.wantErr {
				assert.NotNil(t, err)
			} else {
				assert.Nil(t, err)
			}

			assert.Equal(t, len(tt.advices), len(advices))
			for i, v := range tt.advices {
				assert.Equal(t, v, advices[i])
			}

			assert.Nil(t, m.ExpectationsWereMet())
		})
	}
}

func TestOptimizer_getSelectivity(t *testing.T) {
	type args struct {
		table  string
		column string
	}
	tests := []struct {
		args    args
		wantS   float64
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			o := NewOptimizer(log.NewEntry(), context.NewContext(nil), nil, nil)
			gotS, err := o.getSelectivity(tt.args.table, tt.args.column)
			if tt.wantErr {
				assert.NotNil(t, err)
			} else {
				assert.Nil(t, err)
			}
			assert.Equal(t, tt.wantS, gotS)
		})
	}
}
