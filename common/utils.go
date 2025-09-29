package common

import (
	"fmt"

	"github.com/distribuidos-Coffee-Shop-Analysis/nodes/protocol"
)

// encodeToByteArray encodes batch message to byte array
func EncodeToByteArray(batchMessage *protocol.BatchMessage) []byte {
	// [MessageType][DatasetType][EOF][RecordCount][Records...]
	data := make([]byte, 0)
	data = append(data, protocol.MessageTypeBatch)
	data = append(data, byte(batchMessage.DatasetType))

	// Build content: EOF|RecordCount|Record1|Record2|...
	eofValue := "0"
	if batchMessage.EOF {
		eofValue = "1"
	}

	content := fmt.Sprintf("%s|%d", eofValue, len(batchMessage.Records))
	for _, record := range batchMessage.Records {
		content += "|" + record.Serialize()
	}

	data = append(data, []byte(content)...)
	return data
}