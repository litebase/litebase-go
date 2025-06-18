package sql

type ColumnType int

const (
	ColumnTypeUnknown ColumnType = 0
	ColumnTypeInteger ColumnType = 1
	ColumnTypeFloat   ColumnType = 2
	ColumnTypeText    ColumnType = 3
	ColumnTypeBlob    ColumnType = 4
	ColumnTypeNull    ColumnType = 5
)

type Column struct {
	Type  ColumnType
	Value []byte
}
