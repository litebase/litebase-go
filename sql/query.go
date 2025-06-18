package sql

type Query struct {
	ID            []byte      `json:"id"`
	Statement     string      `json:"statement"`
	Parameters    []Parameter `json:"parameters"`
	TransactionID []byte      `json:"transaction_id"`
}
