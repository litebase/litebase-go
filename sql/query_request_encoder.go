package sql

import (
	"bytes"
	"encoding/binary"
)

func QueryRequestEncoder(
	msg map[string]any,
	outputBuffer *bytes.Buffer,
	parametersBuffer *bytes.Buffer,
) []byte {
	outputBuffer.Reset()
	parametersBuffer.Reset()
	var transactionId string

	id := msg["id"].(string)
	statement := msg["statement"].(string)

	if msg["transactionId"] != nil {
		transactionId = msg["transactionId"].(string)
	}

	if msg["parameters"] != nil {
		msg["parameters"] = []Parameter{}

		for _, parameter := range msg["parameters"].([]Parameter) {
			parameterType := parameter.Type

			// Write the value length
			switch parameterType {
			case "INTEGER":
				binary.Write(parametersBuffer, binary.LittleEndian, uint8(ColumnTypeInteger))
				binary.Write(parametersBuffer, binary.LittleEndian, uint32(8))
				binary.Write(parametersBuffer, binary.LittleEndian, uint64(parameter.Value.(int)))
			case "FLOAT":
			case "REAL":
				binary.Write(parametersBuffer, binary.LittleEndian, uint8(ColumnTypeFloat))
				binary.Write(parametersBuffer, binary.LittleEndian, uint32(8))
				binary.Write(parametersBuffer, binary.LittleEndian, parameter.Value.(float64))
			case "TEXT":
				binary.Write(parametersBuffer, binary.LittleEndian, uint8(ColumnTypeText))
				binary.Write(parametersBuffer, binary.LittleEndian, uint32(len(parameter.Value.(string))))
				parametersBuffer.Write([]byte(parameter.Value.(string)))
			case "BLOB":
				binary.Write(parametersBuffer, binary.LittleEndian, uint8(ColumnTypeBlob))
				binary.Write(parametersBuffer, binary.LittleEndian, uint32(len(parameter.Value.([]byte))))
				parametersBuffer.Write(parameter.Value.([]byte))
			case "NULL":
				binary.Write(parametersBuffer, binary.LittleEndian, uint8(ColumnTypeNull))
				binary.Write(parametersBuffer, binary.LittleEndian, uint32(0))
			}
		}
	}

	// Write the length of the id
	binary.Write(outputBuffer, binary.LittleEndian, uint32(len(id)))

	// Write the id
	outputBuffer.Write([]byte(id))

	// Write the length of the transaction id
	binary.Write(outputBuffer, binary.LittleEndian, uint32(len(transactionId)))

	if transactionId != "" {
		// Write the transaction id
		outputBuffer.Write([]byte(transactionId))
	}

	// Write the length of the statement
	binary.Write(outputBuffer, binary.LittleEndian, uint32(len(statement)))

	// Write the statement
	outputBuffer.Write([]byte(statement))

	// Write the length of the parameters array
	binary.Write(outputBuffer, binary.LittleEndian, uint32(parametersBuffer.Len()))

	// Write the parameters array
	outputBuffer.Write(parametersBuffer.Bytes())

	return outputBuffer.Bytes()
}
