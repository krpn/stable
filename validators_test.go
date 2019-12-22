package stable

import (
	"errors"
	"testing"
)

func TestValidator_IsValid(t *testing.T) {
	t.Parallel()
	validationField := "testField"
	type testTableData struct {
		testCase    string
		validator   validator
		rows        []map[string]string
		expectedErr error
	}
	testTable := []testTableData{
		{
			testCase:  "valueEmptyValidator pass",
			validator: newValueEmptyValidator(validationField),
			rows: []map[string]string{
				{validationField: "1"},
				{validationField: "2"},
			},
			expectedErr: nil,
		},
		{
			testCase:  "valueEmptyValidator error",
			validator: newValueEmptyValidator(validationField),
			rows: []map[string]string{
				{validationField: "1"},
				{validationField: ""},
			},
			expectedErr: errors.New("empty value for field \"testField\""),
		},
		{
			testCase:  "valueDuplicatesValidator pass",
			validator: newValueDuplicatesValidator(validationField),
			rows: []map[string]string{
				{validationField: "1"},
				{validationField: "2"},
			},
			expectedErr: nil,
		},
		{
			testCase:  "valueDuplicatesValidator error",
			validator: newValueDuplicatesValidator(validationField),
			rows: []map[string]string{
				{validationField: "1"},
				{validationField: "1"},
			},
			expectedErr: errors.New("duplicate value \"1\" for field \"testField\""),
		},
	}
	for _, testUnit := range testTable {
		err := testUnit.validator.isValid(testUnit.rows)
		equal(t, testUnit.expectedErr, err, testUnit.testCase)
	}
}
