package sql

type QueryResponse struct {
	Data  QueryResponseData
	Error []byte
}

type QueryResponseData struct {
	Version         byte
	Changes         int64
	Latency         float64
	ColumnsCount    int
	RowsCount       int
	LastInsertRowID int
	ID              []byte
	Columns         []ColumnDefinition
	Rows            [][]Column
	TransactionId   []byte
}
