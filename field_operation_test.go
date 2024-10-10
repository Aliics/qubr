package qubr

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_fieldOperationTree_BuildQuery(t1 *testing.T) {
	type fields struct {
		op  FieldOperation
		or  *fieldOperationTree
		and *fieldOperationTree
	}
	tests := []struct {
		name      string
		fields    fields
		wantQuery string
		wantArgs  []any
	}{
		{
			name: "simple field equality",
			fields: fields{
				op: Equal("Name", "captain spud"),
			},
			wantQuery: ` WHERE "Name" = ?`,
			wantArgs:  []any{"captain spud"},
		},
		{
			name: "chaining multiple equals",
			fields: fields{
				op: Equal("Name", "captain spud"),
				and: &fieldOperationTree{
					op: Equal("Age", 291),
				},
			},
			wantQuery: ` WHERE "Name" = ? AND "Age" = ?`,
			wantArgs:  []any{"captain spud", 291},
		},
		{
			name: "complex tree with many operations",
			fields: fields{
				op: In("FavoriteFood", "kale", "broccoli", "bok choi", "lettuce", "cranberries"),
				and: &fieldOperationTree{
					op: GreaterThanOrEqual("Age", 100),
					or: &fieldOperationTree{
						op: Equal("Deets", 2),
					},
				},
			},
			wantQuery: ` WHERE "FavoriteFood" IN (?, ?, ?, ?, ?) AND "Age" >= ? OR "Deets" = ?`,
			wantArgs:  []any{"kale", "broccoli", "bok choi", "lettuce", "cranberries", 100, 2},
		},
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			t := fieldOperationTree{
				op:  tt.fields.op,
				or:  tt.fields.or,
				and: tt.fields.and,
			}
			gotQuery, gotArgs := t.BuildQuery()
			assert.Equalf(t1, tt.wantQuery, gotQuery, "BuildQuery()")
			assert.Equalf(t1, tt.wantArgs, gotArgs, "BuildQuery()")
		})
	}
}
