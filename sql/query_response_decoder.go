package sql

import (
	"bytes"
	"encoding/binary"
)

type QueryStreamMessageType int

const (
	QueryStreamOpenConnection  QueryStreamMessageType = 0x01
	QueryStreamCloseConnection QueryStreamMessageType = 0x02
	QueryStreamError           QueryStreamMessageType = 0x03
	QueryStreamFrame           QueryStreamMessageType = 0x04
	QueryStreamFrameEntry      QueryStreamMessageType = 0x05
)

func QueryResponseDecoder(buffer *bytes.Buffer) []QueryResponse {
	responses := []QueryResponse{}
	messageType := buffer.Next(1)[0]

	switch QueryStreamMessageType(messageType) {
	case QueryStreamError:
		responseLength := buffer.Next(4)
		response := buffer.Next(int(binary.LittleEndian.Uint32(responseLength)))
		version := response[0]
		offset := 1
		idLength := int(binary.LittleEndian.Uint32(response[offset : offset+4]))
		offset += 4
		id := response[offset : offset+idLength]
		offset += idLength
		transactionIdLength := int(binary.LittleEndian.Uint32(response[offset : offset+4]))
		offset += 4
		transactionId := response[offset : offset+transactionIdLength]
		offset += transactionIdLength
		errorLength := int(binary.LittleEndian.Uint32(response[offset : offset+4]))
		offset += 4
		errorMessage := response[offset : offset+errorLength]

		responses = append(responses, QueryResponse{
			Data: QueryResponseData{
				Version:       version,
				ID:            id,
				TransactionId: transactionId,
			},
			Error: errorMessage,
		})
	case QueryStreamFrameEntry:
		responseLength := buffer.Next(4)
		response := buffer.Next(int(binary.LittleEndian.Uint32(responseLength)))
		version := response[0]
		offset := 1
		idLength := int(binary.LittleEndian.Uint32(response[offset : offset+4]))
		offset += 4
		id := response[offset : offset+idLength]
		offset += idLength
		transactionIdLength := int(binary.LittleEndian.Uint32(response[offset : offset+4]))
		offset += 4
		transactionId := response[offset : offset+transactionIdLength]
		offset += transactionIdLength
		changes := int64(binary.LittleEndian.Uint32(response[offset : offset+4]))
		offset += 4
		latency := float64(binary.LittleEndian.Uint64(response[offset : offset+8]))
		offset += 8
		columnsCount := int(binary.LittleEndian.Uint32(response[offset : offset+4]))
		offset += 4
		rowsCount := int(binary.LittleEndian.Uint32(response[offset : offset+4]))
		offset += 4
		lastInsertRowID := int(binary.LittleEndian.Uint32(response[offset : offset+4]))
		offset += 4
		columnsLength := int(binary.LittleEndian.Uint32(response[offset : offset+4]))
		offset += 4
		columnBytes := response[offset : offset+columnsLength]
		offset += columnsLength
		rowBytes := response[offset:]

		columns := decodeColumns(columnsCount, columnBytes)
		rows := decodeRows(rowsCount, columnsCount, rowBytes)

		responses = append(responses, QueryResponse{
			Data: QueryResponseData{
				Version:         version,
				Changes:         changes,
				Latency:         latency,
				ColumnsCount:    columnsCount,
				RowsCount:       rowsCount,
				LastInsertRowID: lastInsertRowID,
				ID:              id,
				Columns:         columns,
				Rows:            rows,
				TransactionId:   transactionId,
			},
		})
	}

	return responses
}

func decodeColumns(columnCount int, columnsBytes []byte) []ColumnDefinition {
	offset := 0
	columns := make([]ColumnDefinition, columnCount)
	index := 0

	for offset < len(columnsBytes) {
		// Read column name length (4 bytes)
		columnNameLength := int(binary.LittleEndian.Uint32(columnsBytes[offset : offset+4]))
		offset += 4

		// Read column name
		columnName := string(columnsBytes[offset : offset+columnNameLength])
		offset += columnNameLength

		// Read column type (4 bytes, as int32)
		columnType := ColumnType(int32(binary.LittleEndian.Uint32(columnsBytes[offset : offset+4])))
		offset += 4

		columns[index] = ColumnDefinition{
			ColumnName: columnName,
			ColumnType: columnType,
		}
		index++
	}

	return columns
}

func decodeRows(rowsCount, columnsCount int, rowsBytes []byte) [][]Column {
	rowsOffset := 0
	rows := make([][]Column, 0, rowsCount)

	for rowsOffset < len(rowsBytes) {
		rowLength := int(binary.LittleEndian.Uint32(rowsBytes[rowsOffset : rowsOffset+4]))
		rowsOffset += 4
		rowOffset := rowsOffset
		rowsOffset += rowLength

		// Create a new row for each iteration
		currentRow := make([]Column, columnsCount)
		columnIndex := 0

		for rowOffset < rowsOffset {
			columnType := rowsBytes[rowOffset]
			rowOffset++
			columnValueLength := int(binary.LittleEndian.Uint32(rowsBytes[rowOffset : rowOffset+4]))
			rowOffset += 4
			columnValue := rowsBytes[rowOffset : rowOffset+columnValueLength]
			rowOffset += columnValueLength

			var column Column

			column.Type = ColumnType(columnType)
			column.Value = columnValue

			// switch ColumnType(columnType) {
			// case ColumnTypeInteger:
			// 	columnValue
			// 	value = int64(binary.LittleEndian.Uint64(columnValue))
			// case ColumnTypeFloat:
			// 	value = float64(binary.LittleEndian.Uint64(columnValue))
			// case ColumnTypeText:
			// 	value = columnValue
			// case ColumnTypeBlob:
			// 	value = columnValue
			// case ColumnTypeNull:
			// 	value = nil
			// }

			currentRow[columnIndex] = column
			columnIndex++
		}

		rows = append(rows, currentRow)
	}

	return rows
}
