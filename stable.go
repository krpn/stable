package stable

import (
	"database/sql"
	"errors"
	"reflect"
	"sync"
)

// NewSTable creates new STable.
func NewSTable(
	rows []map[string]string,
	primaryKeyField string,
	nonEmptyFields []string,
	uniqFields []string,
) (STable, error) {
	if primaryKeyField == "" {
		return nil, errors.New("primary key is empty")
	}
	vs := []validator{
		newValueEmptyValidator(primaryKeyField),
		newValueDuplicatesValidator(primaryKeyField),
	}
	for _, field := range nonEmptyFields {
		vs = append(vs, newValueEmptyValidator(field))
	}
	for _, field := range uniqFields {
		vs = append(vs, newValueDuplicatesValidator(field))
	}
	st := &stable{primaryKeyField: primaryKeyField, validators: vs, triggers: []Trigger{}}
	return st, st.commit(rows)
}

type stable struct {
	sync.RWMutex
	primaryKeyField string
	rows            []map[string]string
	validators      []validator
	triggers        []Trigger
}

func (st *stable) Insert(new []map[string]string) (int, error) {
	st.Lock()
	defer st.Unlock()
	return st.insert(new)
}

func (st *stable) Upsert(new []map[string]string) (int, error) {
	st.Lock()
	defer st.Unlock()
	return st.upsert(new)
}

func (st *stable) Update(fields map[string]string, where map[string]string) (int, error) {
	st.Lock()
	defer st.Unlock()
	return st.update(fields, where)
}

func (st *stable) Select(where map[string]string) ([]map[string]string, error) {
	st.RLock()
	defer st.RUnlock()
	rows := st.selectRows(where)
	if len(rows) == 0 {
		return nil, sql.ErrNoRows
	}
	return rows, nil
}

func (st *stable) SelectAny(where map[string]string) (map[string]string, error) {
	st.RLock()
	defer st.RUnlock()
	row := st.selectAny(where)
	if row == nil {
		return nil, sql.ErrNoRows
	}
	return row, nil
}

func (st *stable) AddTrigger(trigger Trigger) {
	st.Lock()
	defer st.Unlock()
	st.triggers = append(st.triggers, trigger)
}

func (st *stable) Delete(where map[string]string) (int, error) {
	st.Lock()
	defer st.Unlock()
	return st.delete(where)
}

func (st *stable) insert(new []map[string]string) (int, error) {
	err := st.validateRows(new)
	if err != nil {
		return 0, err
	}
	rows := st.getRowsCopy()
	rows = append(rows, new...)
	err = st.commit(rows)
	if err != nil {
		return 0, err
	}
	return len(new), nil
}

func (st *stable) upsert(new []map[string]string) (int, error) {
	err := st.validateRows(new)
	if err != nil {
		return 0, err
	}
	rows := st.getRowsCopy()
	rows = st.mergeRows(rows, new)
	err = st.commit(rows)
	if err != nil {
		return 0, err
	}
	return len(new), nil
}

func (st *stable) update(fields map[string]string, where map[string]string) (int, error) {
	if _, ok := fields[st.primaryKeyField]; ok {
		return 0, errors.New("update of primary key is forbidden")
	}
	rows := st.selectRows(where)
	if len(rows) == 0 {
		return 0, nil
	}
	for _, row := range rows {
		for field, value := range fields {
			row[field] = value
		}
	}
	return st.upsert(rows)
}

func (st *stable) selectRows(where map[string]string) []map[string]string {
	rows := st.getRowsCopy()
	if len(where) == 0 {
		return rows
	}
	filtered := make([]map[string]string, 0)
rowsLoop:
	for _, row := range rows {
		for field, value := range where {
			rowValue, ok := row[field]
			if !ok || rowValue != value {
				continue rowsLoop
			}
		}
		filtered = append(filtered, row)
	}
	return filtered
}

func (st *stable) selectAny(where map[string]string) map[string]string {
	rows := st.selectRows(where)
	if len(rows) == 0 {
		return nil
	}
	return rows[0]
}

func (st *stable) delete(where map[string]string) (int, error) {
	rowsForDelete := st.selectRows(where)
	if len(rowsForDelete) == 0 {
		return 0, nil
	}
	rows := st.getRowsCopy()
	rows = st.deleteRows(rows, rowsForDelete)
	err := st.commit(rows)
	if err != nil {
		return 0, err
	}
	return len(rowsForDelete), nil
}

func (st *stable) deleteRows(rows []map[string]string, rowsForDelete []map[string]string) []map[string]string {
rowsForDeleteLoop:
	for _, delete := range rowsForDelete {
		deletePK := delete[st.primaryKeyField]
		for i, row := range rows {
			if row[st.primaryKeyField] == deletePK {
				rows = append(rows[:i], rows[i+1:]...)
				continue rowsForDeleteLoop
			}
		}
	}
	return rows
}

func (st *stable) commit(rows []map[string]string) error {
	err := st.validateRows(rows)
	if err != nil {
		return err
	}
	err = st.runTriggers(rows, st.rows)
	if err != nil {
		return err
	}
	st.rows = rows
	return nil
}

func (st *stable) getRowsCopy() []map[string]string {
	cp := make([]map[string]string, len(st.rows))
	for i, row := range st.rows {
		cpRow := make(map[string]string, len(row))
		for field, value := range row {
			cpRow[field] = value
		}
		cp[i] = cpRow
	}
	return cp
}

func (st *stable) validateRows(rows []map[string]string) error {
	for _, validator := range st.validators {
		err := validator.isValid(rows)
		if err != nil {
			return err
		}
	}
	return nil
}

func (st *stable) mergeRows(rows []map[string]string, newRows []map[string]string) []map[string]string {
newRowsLoop:
	for _, newRow := range newRows {
		newPK := newRow[st.primaryKeyField]
		for _, row := range rows {
			if row[st.primaryKeyField] != newPK {
				continue
			}
			for field, value := range newRow {
				row[field] = value
			}
			continue newRowsLoop
		}
		rows = append(rows, newRow)
	}
	return rows
}

func (st *stable) runTriggers(new, old []map[string]string) error {
	if len(st.triggers) == 0 {
		return nil
	}
	err := st.runInsertUpdateTriggers(new, old)
	if err != nil {
		return err
	}
	return st.runDeleteTriggers(new, old)
}

func (st *stable) runInsertUpdateTriggers(new, old []map[string]string) error {
	for _, newRow := range new {
		err := st.runTriggersForNewRow(newRow, old)
		if err != nil {
			return err
		}
	}
	return nil
}

func (st *stable) runTriggersForNewRow(newRow map[string]string, old []map[string]string) error {
	newPK := newRow[st.primaryKeyField]
	for _, oldRow := range old {
		if oldRow[st.primaryKeyField] != newPK {
			continue // another row
		}
		if reflect.DeepEqual(newRow, oldRow) {
			return nil // not changed
		}
		err := st.runTriggersForRow(OperationUpdate, newRow, oldRow)
		if err != nil {
			return err
		}
		return nil
	}
	// not found, inserted
	return st.runTriggersForRow(OperationInsert, newRow, nil)
}

func (st *stable) runDeleteTriggers(new, old []map[string]string) error {
oldRowsLoop:
	for _, oldRow := range old {
		oldPK := oldRow[st.primaryKeyField]
		for _, newRow := range new {
			if newRow[st.primaryKeyField] == oldPK {
				continue oldRowsLoop
			}
		}
		err := st.runTriggersForRow(OperationDelete, nil, oldRow)
		if err != nil {
			return err
		}
	}
	return nil
}

func (st *stable) runTriggersForRow(operation int, new, old map[string]string) error {
	for _, trigger := range st.triggers {
		err := trigger.Handle(operation, new, old)
		if err != nil {
			return err
		}
	}
	return nil
}
