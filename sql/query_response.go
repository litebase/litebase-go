package sql

type QueryResponse struct {
	Data  QueryresponseData
	Error []byte
}

type QueryresponseData struct {
	Version         byte
	Changes         int64
	Latency         float64
	ColumnsCount    int
	RowsCount       int
	LastInsertRowID int
	ID              []byte
	Columns         [][]byte
	Rows            [][]Column
	TransactionId   []byte
}
