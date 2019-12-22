package stable

import (
	"fmt"
)

type validator interface {
	isValid(rows []map[string]string) error
}

type valueEmptyValidator struct {
	field string
}

func newValueEmptyValidator(field string) validator {
	return &valueEmptyValidator{
		field: field,
	}
}

func (f *valueEmptyValidator) isValid(rows []map[string]string) error {
	for _, row := range rows {
		value := row[f.field]
		if value == "" {
			return fmt.Errorf("empty value for field \"%v\"", f.field)
		}
	}
	return nil
}

type valueDuplicatesValidator struct {
	field string
}

func newValueDuplicatesValidator(field string) validator {
	return &valueDuplicatesValidator{
		field: field,
	}
}

func (f *valueDuplicatesValidator) isValid(rows []map[string]string) error {
	find := make(map[string]struct{})
	for _, row := range rows {
		value := row[f.field]
		if value == "" {
			continue
		}
		if _, ok := find[value]; ok {
			return fmt.Errorf("duplicate value \"%v\" for field \"%v\"", value, f.field)
		}
		find[value] = struct{}{}
	}
	return nil
}
