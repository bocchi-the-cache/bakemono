package bakemono

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
)

const (
	ChunkHeaderSizeFixed = 8 * 1 << 10 // 8KB
	ChunkKeyMaxSize      = 4 * 1 << 10 // 4KB
	ChunkDataSize        = 1 * 1 << 20 // 1MB
)

// Chunk is the unit of data storage.
// Contains a header(meta) and data.
type Chunk struct {
	Header  ChunkHeader
	DataRaw []byte
}

// Set sets the key and data of the chunk.
func (c *Chunk) Set(key, data []byte) error {
	if len(data) > ChunkDataSize {
		return ErrChunkDataTooLarge
	}
	if len(key) > ChunkKeyMaxSize {
		return ErrChunkKeyTooLarge
	}
	c.DataRaw = data
	copy(c.Header.Key[:], key)
	c.Header.DataLength = uint32(len(data))
	c.Header.HeaderSize = ChunkHeaderSizeFixed
	c.Header.Checksum = crc32.ChecksumIEEE(data)
	return nil
}

// GetKeyData returns the key and data of the chunk.
// Note: The key is trimmed by the null character.
func (c *Chunk) GetKeyData() ([]byte, []byte) {
	keyTrim := bytes.TrimRight(c.Header.Key[:], "\x00")
	return keyTrim, c.DataRaw
}

// WriteAt writes the chunk to the writer at the offset.
func (c *Chunk) WriteAt(w io.WriterAt, off int64) error {
	b, err := c.MarshalBinary()
	if err != nil {
		return err
	}
	_, err = w.WriteAt(b, off)
	return err
}

// Verify verifies the chunk. It returns nil if the chunk is valid.
func (c *Chunk) Verify() error {
	// data length check
	if len(c.DataRaw) != int(c.Header.DataLength) {
		return ErrChunkVerifyFailed
	}
	// checksum check data
	if crc := crc32.ChecksumIEEE(c.DataRaw); crc != c.Header.Checksum {
		return ErrChunkVerifyFailed
	}
	return nil
}

// MarshalBinary returns the binary of the chunk.
func (c *Chunk) MarshalBinary() ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0, ChunkHeaderSizeFixed+len(c.DataRaw)))
	b, err := c.Header.MarshalBinary()
	if err != nil {
		return nil, err
	}
	buf.Write(b)
	// padding to ChunkHeaderSizeFixed
	buf.Write(make([]byte, ChunkHeaderSizeFixed-len(b)))
	buf.Write(c.DataRaw)
	return buf.Bytes(), nil
}

// UnmarshalBinary unmarshal the binary of the chunk, and verify it.
// Note: the data must be the whole chunk.
func (c *Chunk) UnmarshalBinary(data []byte) error {
	buf := bytes.NewBuffer(data)
	if err := c.Header.UnmarshalBinary(buf.Next(ChunkHeaderSizeFixed)); err != nil {
		return err
	}
	c.DataRaw = buf.Next(int(c.Header.DataLength))
	return c.Verify()
}

// ChunkHeader is the meta of a chunk.
type ChunkHeader struct {
	Checksum   uint32
	Key        [ChunkKeyMaxSize]byte
	DataLength uint32
	HeaderSize uint32
	//HeaderChecksum uint32
}

// MarshalBinary returns the binary representation of the chunk header.
// TODO: could use a buffer pool to avoid allocating a new buffer every time.
func (c *ChunkHeader) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.BigEndian, *c); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// UnmarshalBinary unmarshal the binary representation of the chunk header.
func (c *ChunkHeader) UnmarshalBinary(data []byte) error {
	return binary.Read(bytes.NewBuffer(data), binary.BigEndian, c)
}
