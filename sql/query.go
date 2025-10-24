package sql

type Query struct {
	ID            string      `json:"id"`
	Statement     string      `json:"statement"`
	Parameters    []Parameter `json:"parameters"`
	TransactionID string      `json:"transactionId"`
}
