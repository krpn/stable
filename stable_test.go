package stable

import (
	"database/sql"
	"errors"
	"reflect"
	"testing"
)

func Test_NewSTable(t *testing.T) {
	t.Parallel()
	_, err := NewSTable(nil, "", nil, nil)
	equal(t, errors.New("primary key is empty"), err, "error ErrEmptyPrimaryKey throwed")
}

func TestSTable_InsertUpsert(t *testing.T) {
	t.Parallel()
	initRows := []map[string]string{
		{"pk": "0", "nonEmpty": "e0", "uniq": "u0"},
	}
	primaryKeyField := "pk"
	nonEmptyFields := []string{"nonEmpty"}
	uniqFields := []string{"uniq"}
	type testTableData struct {
		testCase               string
		rows                   []map[string]string
		expectedAffected       int
		expectedErr            error
		expectedTriggerRecords []testTriggerRecord
		expectedSelected       []map[string]string
	}
	testTable := []testTableData{
		{
			testCase: "one row",
			rows: []map[string]string{
				{"pk": "1", "nonEmpty": "e1", "uniq": "u1"},
			},
			expectedAffected: 1,
			expectedErr:      nil,
			expectedTriggerRecords: []testTriggerRecord{
				{operation: OperationInsert, new: map[string]string{"pk": "1", "nonEmpty": "e1", "uniq": "u1"}, old: nil},
			},
			expectedSelected: []map[string]string{
				{"pk": "0", "nonEmpty": "e0", "uniq": "u0"},
				{"pk": "1", "nonEmpty": "e1", "uniq": "u1"},
			},
		},
		{
			testCase: "two rows",
			rows: []map[string]string{
				{"pk": "1", "nonEmpty": "e1", "uniq": "u1"},
				{"pk": "2", "nonEmpty": "e2", "uniq": "u2"},
			},
			expectedAffected: 2,
			expectedErr:      nil,
			expectedTriggerRecords: []testTriggerRecord{
				{operation: OperationInsert, new: map[string]string{"pk": "1", "nonEmpty": "e1", "uniq": "u1"}, old: nil},
				{operation: OperationInsert, new: map[string]string{"pk": "2", "nonEmpty": "e2", "uniq": "u2"}, old: nil},
			},
			expectedSelected: []map[string]string{
				{"pk": "0", "nonEmpty": "e0", "uniq": "u0"},
				{"pk": "1", "nonEmpty": "e1", "uniq": "u1"},
				{"pk": "2", "nonEmpty": "e2", "uniq": "u2"},
			},
		},
		{
			testCase: "empty PK",
			rows: []map[string]string{
				{"nonEmpty": "e1", "uniq": "u1"},
			},
			expectedAffected:       0,
			expectedErr:            errors.New("empty value for field \"pk\""),
			expectedTriggerRecords: nil,
			expectedSelected: []map[string]string{
				{"pk": "0", "nonEmpty": "e0", "uniq": "u0"},
			},
		},
		{
			testCase: "empty non empty field",
			rows: []map[string]string{
				{"pk": "1", "uniq": "u1"},
			},
			expectedAffected:       0,
			expectedErr:            errors.New("empty value for field \"nonEmpty\""),
			expectedTriggerRecords: nil,
			expectedSelected: []map[string]string{
				{"pk": "0", "nonEmpty": "e0", "uniq": "u0"},
			},
		},
		{
			testCase: "duplicate uniq field",
			rows: []map[string]string{
				{"pk": "1", "nonEmpty": "e1", "uniq": "u0"},
			},
			expectedAffected:       0,
			expectedErr:            errors.New("duplicate value \"u0\" for field \"uniq\""),
			expectedTriggerRecords: nil,
			expectedSelected: []map[string]string{
				{"pk": "0", "nonEmpty": "e0", "uniq": "u0"},
			},
		},
		{
			testCase: "trigger error",
			rows: []map[string]string{
				{"pk": "triggerError", "nonEmpty": "e1", "uniq": "u1"},
			},
			expectedAffected:       0,
			expectedErr:            errors.New("trigger error"),
			expectedTriggerRecords: nil,
			expectedSelected: []map[string]string{
				{"pk": "0", "nonEmpty": "e0", "uniq": "u0"},
			},
		},
	}
	// Insert
	for _, testUnit := range testTable {
		s, err := NewSTable(initRows, primaryKeyField, nonEmptyFields, uniqFields)
		if err != nil {
			t.Fatal(err)
		}
		trigger := newTestTrigger(primaryKeyField, "triggerError")
		s.AddTrigger(trigger)
		affected, err := s.Insert(testUnit.rows)
		equal(t, testUnit.expectedAffected, affected, testUnit.testCase)
		equal(t, testUnit.expectedErr, err, testUnit.testCase)
		equal(t, testUnit.expectedTriggerRecords, trigger.getRecords(), testUnit.testCase)
		selected, err := s.Select(nil)
		if err != nil {
			t.Fatal(err)
		}
		equal(t, testUnit.expectedSelected, selected, testUnit.testCase)
	}
	// Upsert
	for _, testUnit := range testTable {
		s, err := NewSTable(initRows, primaryKeyField, nonEmptyFields, uniqFields)
		if err != nil {
			t.Fatal(err)
		}
		trigger := newTestTrigger(primaryKeyField, "triggerError")
		s.AddTrigger(trigger)
		affected, err := s.Upsert(testUnit.rows)
		equal(t, testUnit.expectedAffected, affected, testUnit.testCase)
		equal(t, testUnit.expectedErr, err, testUnit.testCase)
		equal(t, testUnit.expectedTriggerRecords, trigger.getRecords(), testUnit.testCase)
		selected, err := s.Select(nil)
		if err != nil {
			t.Fatal(err)
		}
		equal(t, testUnit.expectedSelected, selected, testUnit.testCase)
	}
}

func TestSTable_Upsert(t *testing.T) {
	t.Parallel()
	primaryKeyField := "pk"
	nonEmptyFields := []string{"nonEmpty"}
	uniqFields := []string{"uniq"}
	type testTableData struct {
		testCase               string
		initRows               []map[string]string
		rows                   []map[string]string
		expectedAffected       int
		expectedErr            error
		expectedTriggerRecords []testTriggerRecord
		expectedSelected       []map[string]string
	}
	testTable := []testTableData{
		{
			testCase: "partial update of one row",
			initRows: []map[string]string{
				{"pk": "0", "nonEmpty": "e0", "uniq": "u0"},
				{"pk": "1", "nonEmpty": "e1", "uniq": "u1"},
			},
			rows: []map[string]string{
				{"pk": "1", "nonEmpty": "e11", "uniq": "u11"},
			},
			expectedAffected: 1,
			expectedErr:      nil,
			expectedTriggerRecords: []testTriggerRecord{
				{
					operation: OperationUpdate,
					new:       map[string]string{"pk": "1", "nonEmpty": "e11", "uniq": "u11"},
					old:       map[string]string{"pk": "1", "nonEmpty": "e1", "uniq": "u1"},
				},
			},
			expectedSelected: []map[string]string{
				{"pk": "0", "nonEmpty": "e0", "uniq": "u0"},
				{"pk": "1", "nonEmpty": "e11", "uniq": "u11"},
			},
		},
		{
			testCase: "partial update of one row + insert",
			initRows: []map[string]string{
				{"pk": "0", "nonEmpty": "e0", "uniq": "u0"},
				{"pk": "1", "nonEmpty": "e1", "uniq": "u1"},
			},
			rows: []map[string]string{
				{"pk": "1", "nonEmpty": "e11", "new": "n11"},
				{"pk": "2", "nonEmpty": "e2", "uniq": "u2"},
			},
			expectedAffected: 2,
			expectedErr:      nil,
			expectedTriggerRecords: []testTriggerRecord{
				{
					operation: OperationUpdate,
					new:       map[string]string{"pk": "1", "nonEmpty": "e11", "uniq": "u1", "new": "n11"},
					old:       map[string]string{"pk": "1", "nonEmpty": "e1", "uniq": "u1"},
				},
				{
					operation: OperationInsert,
					new:       map[string]string{"pk": "2", "nonEmpty": "e2", "uniq": "u2"},
					old:       nil,
				},
			},
			expectedSelected: []map[string]string{
				{"pk": "0", "nonEmpty": "e0", "uniq": "u0"},
				{"pk": "1", "nonEmpty": "e11", "uniq": "u1", "new": "n11"},
				{"pk": "2", "nonEmpty": "e2", "uniq": "u2"},
			},
		},
		{
			testCase: "partial update of one row + insert uniq error",
			initRows: []map[string]string{
				{"pk": "0", "nonEmpty": "e0", "uniq": "u0"},
				{"pk": "1", "nonEmpty": "e1", "uniq": "u1"},
			},
			rows: []map[string]string{
				{"pk": "1", "nonEmpty": "e2", "uniq": "u2"},
				{"pk": "2", "nonEmpty": "e2", "uniq": "u2"},
			},
			expectedAffected:       0,
			expectedErr:            errors.New("duplicate value \"u2\" for field \"uniq\""),
			expectedTriggerRecords: nil,
			expectedSelected: []map[string]string{
				{"pk": "0", "nonEmpty": "e0", "uniq": "u0"},
				{"pk": "1", "nonEmpty": "e1", "uniq": "u1"},
			},
		},
		{
			testCase: "trigger error",
			initRows: []map[string]string{
				{"pk": "triggerError", "nonEmpty": "e2", "uniq": "u2"},
			},
			rows: []map[string]string{
				{"pk": "triggerError", "nonEmpty": "e2", "uniq": "u0"},
			},
			expectedAffected:       0,
			expectedErr:            errors.New("trigger error"),
			expectedTriggerRecords: nil,
			expectedSelected: []map[string]string{
				{"pk": "triggerError", "nonEmpty": "e2", "uniq": "u2"},
			},
		},
	}
	for _, testUnit := range testTable {
		s, err := NewSTable(testUnit.initRows, primaryKeyField, nonEmptyFields, uniqFields)
		if err != nil {
			t.Fatal(err)
		}
		trigger := newTestTrigger(primaryKeyField, "triggerError")
		s.AddTrigger(trigger)
		affected, err := s.Upsert(testUnit.rows)
		equal(t, testUnit.expectedAffected, affected, testUnit.testCase)
		equal(t, testUnit.expectedErr, err, testUnit.testCase)
		equal(t, testUnit.expectedTriggerRecords, trigger.getRecords(), testUnit.testCase)
		selected, err := s.Select(nil)
		if err != nil {
			t.Fatal(err)
		}
		equal(t, testUnit.expectedSelected, selected, testUnit.testCase)
	}
}

func TestSTable_Update(t *testing.T) {
	t.Parallel()
	primaryKeyField := "pk"
	nonEmptyFields := []string{}
	uniqFields := []string{}
	type testTableData struct {
		testCase               string
		initRows               []map[string]string
		fields                 map[string]string
		where                  map[string]string
		expectedAffected       int
		expectedErr            error
		expectedTriggerRecords []testTriggerRecord
		expectedSelected       []map[string]string
	}
	testTable := []testTableData{
		{
			testCase: "update all",
			initRows: []map[string]string{
				{"pk": "0", "forUpdate": "fu0"},
				{"pk": "1", "forUpdate": "fu1"},
			},
			fields:           map[string]string{"forUpdate": "fu000"},
			where:            nil,
			expectedAffected: 2,
			expectedErr:      nil,
			expectedTriggerRecords: []testTriggerRecord{
				{
					operation: OperationUpdate,
					new:       map[string]string{"pk": "0", "forUpdate": "fu000"},
					old:       map[string]string{"pk": "0", "forUpdate": "fu0"},
				},
				{
					operation: OperationUpdate,
					new:       map[string]string{"pk": "1", "forUpdate": "fu000"},
					old:       map[string]string{"pk": "1", "forUpdate": "fu1"},
				},
			},
			expectedSelected: []map[string]string{
				{"pk": "0", "forUpdate": "fu000"},
				{"pk": "1", "forUpdate": "fu000"},
			},
		},
		{
			testCase: "update one row by primary key",
			initRows: []map[string]string{
				{"pk": "0", "forUpdate": "fu0"},
				{"pk": "1", "forUpdate": "fu1"},
			},
			fields:           map[string]string{"forUpdate": "fu000"},
			where:            map[string]string{"pk": "0"},
			expectedAffected: 1,
			expectedErr:      nil,
			expectedTriggerRecords: []testTriggerRecord{
				{
					operation: OperationUpdate,
					new:       map[string]string{"pk": "0", "forUpdate": "fu000"},
					old:       map[string]string{"pk": "0", "forUpdate": "fu0"},
				},
			},
			expectedSelected: []map[string]string{
				{"pk": "0", "forUpdate": "fu000"},
				{"pk": "1", "forUpdate": "fu1"},
			},
		},
		{
			testCase: "update one row by field",
			initRows: []map[string]string{
				{"pk": "0", "forUpdate": "fu0"},
				{"pk": "1", "forUpdate": "fu1"},
			},
			fields:           map[string]string{"forUpdate": "fu000"},
			where:            map[string]string{"forUpdate": "fu0"},
			expectedAffected: 1,
			expectedErr:      nil,
			expectedTriggerRecords: []testTriggerRecord{
				{
					operation: OperationUpdate,
					new:       map[string]string{"pk": "0", "forUpdate": "fu000"},
					old:       map[string]string{"pk": "0", "forUpdate": "fu0"},
				},
			},
			expectedSelected: []map[string]string{
				{"pk": "0", "forUpdate": "fu000"},
				{"pk": "1", "forUpdate": "fu1"},
			},
		},
		{
			testCase: "update one row without change",
			initRows: []map[string]string{
				{"pk": "0", "forUpdate": "fu0"},
				{"pk": "1", "forUpdate": "fu1"},
			},
			fields:                 map[string]string{"forUpdate": "fu0"},
			where:                  map[string]string{"pk": "0"},
			expectedAffected:       1,
			expectedErr:            nil,
			expectedTriggerRecords: nil,
			expectedSelected: []map[string]string{
				{"pk": "0", "forUpdate": "fu0"},
				{"pk": "1", "forUpdate": "fu1"},
			},
		},
		{
			testCase: "update zero rows by existing field",
			initRows: []map[string]string{
				{"pk": "0"},
				{"pk": "1"},
			},
			fields:                 map[string]string{"forUpdate": "fu000"},
			where:                  map[string]string{"pk": "3"},
			expectedAffected:       0,
			expectedErr:            nil,
			expectedTriggerRecords: nil,
			expectedSelected: []map[string]string{
				{"pk": "0"},
				{"pk": "1"},
			},
		},
		{
			testCase: "update zero rows by empty non existing field",
			initRows: []map[string]string{
				{"pk": "0"},
				{"pk": "1"},
			},
			fields:                 map[string]string{"forUpdate": "fu000"},
			where:                  map[string]string{"notExistingField": ""},
			expectedAffected:       0,
			expectedErr:            nil,
			expectedTriggerRecords: nil,
			expectedSelected: []map[string]string{
				{"pk": "0"},
				{"pk": "1"},
			},
		},
		{
			testCase: "update primary key",
			initRows: []map[string]string{
				{"pk": "0"},
				{"pk": "1"},
			},
			fields:                 map[string]string{"pk": "2"},
			where:                  map[string]string{"pk": "0"},
			expectedAffected:       0,
			expectedErr:            errors.New("update of primary key is forbidden"),
			expectedTriggerRecords: nil,
			expectedSelected: []map[string]string{
				{"pk": "0"},
				{"pk": "1"},
			},
		},
	}
	for _, testUnit := range testTable {
		s, err := NewSTable(testUnit.initRows, primaryKeyField, nonEmptyFields, uniqFields)
		if err != nil {
			t.Fatal(err)
		}
		trigger := newTestTrigger(primaryKeyField, "triggerError")
		s.AddTrigger(trigger)
		affected, err := s.Update(testUnit.fields, testUnit.where)
		equal(t, testUnit.expectedAffected, affected, testUnit.testCase)
		equal(t, testUnit.expectedErr, err, testUnit.testCase)
		equal(t, testUnit.expectedTriggerRecords, trigger.getRecords(), testUnit.testCase)
		selected, err := s.Select(nil)
		if err != nil {
			t.Fatal(err)
		}
		equal(t, testUnit.expectedSelected, selected, testUnit.testCase)
	}
}

func TestSTable_SelectSelectAny(t *testing.T) {
	t.Parallel()
	primaryKeyField := "pk"
	nonEmptyFields := []string{}
	uniqFields := []string{}
	type testTableData struct {
		testCase         string
		initRows         []map[string]string
		where            map[string]string
		expectedSelected []map[string]string
		expectedErr      error
	}
	testTable := []testTableData{
		{
			testCase: "select all",
			initRows: []map[string]string{
				{"pk": "0"},
				{"pk": "1"},
			},
			where: nil,
			expectedSelected: []map[string]string{
				{"pk": "0"},
				{"pk": "1"},
			},
			expectedErr: nil,
		},
		{
			testCase: "select one",
			initRows: []map[string]string{
				{"pk": "0"},
				{"pk": "1"},
			},
			where: map[string]string{"pk": "0"},
			expectedSelected: []map[string]string{
				{"pk": "0"},
			},
			expectedErr: nil,
		},
		{
			testCase: "select one by multiple conditions",
			initRows: []map[string]string{
				{"pk": "0", "f1": "v0"},
				{"pk": "1", "f1": "v0"},
			},
			where: map[string]string{"pk": "0", "f1": "v0"},
			expectedSelected: []map[string]string{
				{"pk": "0", "f1": "v0"},
			},
			expectedErr: nil,
		},
		{
			testCase: "select none",
			initRows: []map[string]string{
				{"pk": "0"},
				{"pk": "1"},
			},
			where:            map[string]string{"pk": "2"},
			expectedSelected: nil,
			expectedErr:      sql.ErrNoRows,
		},
	}
	for _, testUnit := range testTable {
		s, err := NewSTable(testUnit.initRows, primaryKeyField, nonEmptyFields, uniqFields)
		if err != nil {
			t.Fatal(err)
		}
		selected, err := s.Select(testUnit.where)
		equal(t, testUnit.expectedErr, err, testUnit.testCase)
		equal(t, testUnit.expectedSelected, selected, testUnit.testCase)
		row, err := s.SelectAny(testUnit.where)
		equal(t, testUnit.expectedErr, err, testUnit.testCase)
		if len(testUnit.expectedSelected) != 0 {
			equal(t, testUnit.expectedSelected[0], row, testUnit.testCase)
		}
	}
}

func TestSTable_Delete(t *testing.T) {
	t.Parallel()
	primaryKeyField := "pk"
	nonEmptyFields := []string{}
	uniqFields := []string{}
	type testTableData struct {
		testCase               string
		initRows               []map[string]string
		where                  map[string]string
		expectedAffected       int
		expectedErr            error
		expectedTriggerRecords []testTriggerRecord
		expectedSelected       []map[string]string
		expectedSelectErr      error
	}
	testTable := []testTableData{
		{
			testCase: "delete all",
			initRows: []map[string]string{
				{"pk": "0"},
				{"pk": "1"},
			},
			where:            nil,
			expectedAffected: 2,
			expectedErr:      nil,
			expectedTriggerRecords: []testTriggerRecord{
				{
					operation: OperationDelete,
					new:       nil,
					old:       map[string]string{"pk": "0"},
				},
				{
					operation: OperationDelete,
					new:       nil,
					old:       map[string]string{"pk": "1"},
				},
			},
			expectedSelected:  nil,
			expectedSelectErr: sql.ErrNoRows,
		},
		{
			testCase: "delete one",
			initRows: []map[string]string{
				{"pk": "0"},
				{"pk": "1"},
			},
			where:            map[string]string{"pk": "0"},
			expectedAffected: 1,
			expectedErr:      nil,
			expectedTriggerRecords: []testTriggerRecord{
				{
					operation: OperationDelete,
					new:       nil,
					old:       map[string]string{"pk": "0"},
				},
			},
			expectedSelected: []map[string]string{
				{"pk": "1"},
			},
			expectedSelectErr: nil,
		},
		{
			testCase: "delete one by multiple conditions",
			initRows: []map[string]string{
				{"pk": "0", "f1": "v0"},
				{"pk": "1", "f1": "v0"},
			},
			where:            map[string]string{"pk": "0", "f1": "v0"},
			expectedAffected: 1,
			expectedErr:      nil,
			expectedTriggerRecords: []testTriggerRecord{
				{
					operation: OperationDelete,
					new:       nil,
					old:       map[string]string{"pk": "0", "f1": "v0"},
				},
			},
			expectedSelected: []map[string]string{
				{"pk": "1", "f1": "v0"},
			},
			expectedSelectErr: nil,
		},
		{
			testCase: "delete none",
			initRows: []map[string]string{
				{"pk": "0"},
				{"pk": "1"},
			},
			where:                  map[string]string{"pk": "2"},
			expectedAffected:       0,
			expectedErr:            nil,
			expectedTriggerRecords: nil,
			expectedSelected: []map[string]string{
				{"pk": "0"},
				{"pk": "1"},
			},
			expectedSelectErr: nil,
		},
		{
			testCase: "trigger error",
			initRows: []map[string]string{
				{"pk": "triggerError"},
			},
			where:                  map[string]string{"pk": "triggerError"},
			expectedAffected:       0,
			expectedErr:            errors.New("trigger error"),
			expectedTriggerRecords: nil,
			expectedSelected: []map[string]string{
				{"pk": "triggerError"},
			},
			expectedSelectErr: nil,
		},
	}
	for _, testUnit := range testTable {
		s, err := NewSTable(testUnit.initRows, primaryKeyField, nonEmptyFields, uniqFields)
		if err != nil {
			t.Fatal(err)
		}
		trigger := newTestTrigger(primaryKeyField, "triggerError")
		s.AddTrigger(trigger)
		affected, err := s.Delete(testUnit.where)
		equal(t, testUnit.expectedErr, err, testUnit.testCase)
		equal(t, testUnit.expectedAffected, affected, testUnit.testCase)
		equal(t, testUnit.expectedTriggerRecords, trigger.getRecords(), testUnit.testCase)
		selected, err := s.Select(nil)
		equal(t, testUnit.expectedSelected, selected, testUnit.testCase)
		equal(t, testUnit.expectedSelectErr, err, testUnit.testCase)
	}
}

type testTriggerRecord struct {
	operation int
	new, old  map[string]string
}

type testTrigger struct {
	records              []testTriggerRecord
	primaryKeyField      string
	primaryKeyValueError string
}

func newTestTrigger(primaryKeyField string, primaryKeyValueError string) *testTrigger {
	return &testTrigger{primaryKeyField: primaryKeyField, primaryKeyValueError: primaryKeyValueError}
}

func (tt *testTrigger) Handle(operation int, new, old map[string]string) error {
	if (new != nil && new[tt.primaryKeyField] == tt.primaryKeyValueError) ||
		(old != nil && old[tt.primaryKeyField] == tt.primaryKeyValueError) {
		return errors.New("trigger error")
	}
	tt.records = append(tt.records, testTriggerRecord{operation: operation, new: new, old: old})
	return nil
}

func (tt *testTrigger) getRecords() []testTriggerRecord {
	return tt.records
}

func equal(t *testing.T, expected, actual interface{}, testCase string) {
	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("Fail %v\nexpected = %v\nactual = %v", testCase, expected, actual)
	}
}
