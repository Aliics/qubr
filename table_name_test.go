package qubr

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

func Test_newTableNameFromString(t *testing.T) {
	type args struct {
		s string
	}
	tests := []struct {
		name    string
		args    args
		want    *tableName
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name:    "nothing provided",
			args:    args{""},
			want:    nil,
			wantErr: assert.Error,
		},
		{
			name:    "table name only",
			args:    args{"bunnies"},
			want:    &tableName{tableName: "bunnies"},
			wantErr: assert.NoError,
		},
		{
			name:    "table and schema",
			args:    args{"bunny_land.bunnies"},
			want:    &tableName{schema: "bunny_land", tableName: "bunnies"},
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := newTableNameFromString(tt.args.s)
			if !tt.wantErr(t, err, fmt.Sprintf("newTableNameFromString(%v)", tt.args.s)) {
				return
			}
			assert.Equalf(t, tt.want, got, "newTableNameFromString(%v)", tt.args.s)
		})
	}
}

func Test_tableName_String(t1 *testing.T) {
	type bunny struct { /* Won't be used. */
	}

	type fields struct {
		forType   reflect.Type
		schema    string
		tableName string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "forType only",
			fields: fields{
				forType: reflect.TypeFor[bunny](),
			},
			want: `"bunny"`,
		},
		{
			name: "tableName only",
			fields: fields{
				tableName: "bunnies",
			},
			want: `"bunnies"`,
		},
		{
			name: "schema and tableName only",
			fields: fields{
				schema:    "bunny_land",
				tableName: "bunnies",
			},
			want: `"bunny_land"."bunnies"`,
		},
		{
			name: "forType, schema and tableName",
			fields: fields{
				forType:   reflect.TypeFor[bunny](),
				schema:    "bunny_land",
				tableName: "bunnies",
			},
			want: `"bunny_land"."bunnies"`, // forType should be ignored.
		},
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			t := tableName{
				forType:   tt.fields.forType,
				schema:    tt.fields.schema,
				tableName: tt.fields.tableName,
			}
			assert.Equalf(t1, tt.want, t.String(), "String()")
		})
	}
}
