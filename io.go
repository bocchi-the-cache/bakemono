package bakemono

import "io"

type OffsetReaderWriterCloser interface {
	io.WriterAt
	io.ReaderAt
	io.Closer
}
