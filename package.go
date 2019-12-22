package stable

// STable represents simple string table engine.
// All rows are stored as a map[string]string.
// Primary key and constraints are supported.
// Triggers are supported.
//
// It is safe calling STable methods from concurrently running goroutines.
type STable interface {
	// Insert inserts rows with constraints checks.
	Insert(rows []map[string]string) (int, error)

	// Insert inserts or updates rows (based on primary key) with constraints checks.
	// Fields of updated rows will be merged instead of row to be fully replaced.
	Upsert(rows []map[string]string) (int, error)

	// Update updates rows based on conditions with constraints checks.
	// Primary key if forbidden to update (you may use delete + insert).
	Update(fields map[string]string, where map[string]string) (int, error)

	// Delete deletes rows by conditions.
	Delete(where map[string]string) (int, error)

	// Select selects rows by conditions.
	// sql.ErrNoRows will be throwed when no rows found.
	Select(where map[string]string) (rows []map[string]string, err error)

	// SelectAny selects one random row by conditions.
	// sql.ErrNoRows will be throwed when no rows found.
	SelectAny(where map[string]string) (row map[string]string, err error)

	// AddTrigger adds trigger to STable.
	AddTrigger(trigger Trigger)
}

const (
	// OperationInsert represents insert event constant for a Trigger.
	OperationInsert = iota
	// OperationUpdate represents update event constant for a Trigger.
	OperationUpdate
	// OperationDelete represents delete event constant for a Trigger.
	OperationDelete
)

// Trigger is a Handler called on STable events.
// Note: trigger not called when updated row is not changed.
type Trigger interface {
	Handle(operation int, new, old map[string]string) error
}
