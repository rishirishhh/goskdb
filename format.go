package caskdb

import (
	"bytes"
	"encoding/binary"
)

const headerSize = 12

// KeyEntry keeps the metadata about the KV, specially the position of
// the byte offset in the file. Whenever we insert/update a key, we create a new
// KeyEntry object and insert that into keyDir.
type KeyEntry struct {
}

func NewKeyEntry(timestamp uint32, position uint32, totalSize uint32) KeyEntry {
	panic("implement me")
}

func encodeHeader(timestamp uint32, keySize uint32, valueSize uint32) []byte {
	// Allocate 12 bytes (4 bytes each for timestamp, keySize, and valueSize)
	buf := make([]byte, 12)

	// Encode each value into the byte slice using LittleEndian encoding
	binary.LittleEndian.PutUint32(buf[0:4], timestamp)
	binary.LittleEndian.PutUint32(buf[4:8], keySize)
	binary.LittleEndian.PutUint32(buf[8:12], valueSize)

	return buf

}

func decodeHeader(header []byte) (uint32, uint32, uint32) {
	// Decode each value from the byte slice using LittleEndian encoding
	timestamp := binary.LittleEndian.Uint32(header[0:4])
	keySize := binary.LittleEndian.Uint32(header[4:8])
	valueSize := binary.LittleEndian.Uint32(header[8:12])

	return timestamp, keySize, valueSize
}

func encodeKV(timestamp uint32, key, value string) (int, []byte) {
	// Calculate sizes
	keySize := len(key)
	valueSize := len(value)
	
	// Total size
	size := headerSize + keySize + valueSize
	buffer := make([]byte, size)

	// Create a buffer to write data
	buf := bytes.NewBuffer(buffer[:0])

	// Write timestamp
	binary.Write(buf, binary.LittleEndian, timestamp)

	// Write key size and value size
	binary.Write(buf, binary.LittleEndian, uint32(keySize))
	binary.Write(buf, binary.LittleEndian, uint32(valueSize))

	// Write the key and value
	buf.WriteString(key)
	buf.WriteString(value)

	return size, buf.Bytes()
}

func decodeKV(data []byte) (uint32, string, string) {
	// Create a buffer to read data
	buf := bytes.NewReader(data)

	var timestamp, keySize, valueSize uint32
	
	// Read the timestamp
	binary.Read(buf, binary.LittleEndian, &timestamp)

	// Read key size and value size
	binary.Read(buf, binary.LittleEndian, &keySize)
	binary.Read(buf, binary.LittleEndian, &valueSize)

	// Read key
	key := make([]byte, keySize)
	buf.Read(key)

	// Read value
	value := make([]byte, valueSize)
	buf.Read(value)

	return timestamp, string(key), string(value)
}
