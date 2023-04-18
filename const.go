package bakemono

const (
	SectorSize = 512
)

// Dir constants
const (
	DirDataSizeLv0 = SectorSize << (0 * 3)              // 512B
	DirDataSizeLv1 = SectorSize << (1 * 3)              // 4KB
	DirDataSizeLv2 = SectorSize << (2 * 3)              // 32KB
	DirDataSizeLv3 = SectorSize << (3 * 3)              // 256KB
	DirMaxDataSize = (SectorSize << (3 * 3)) * (1 << 6) //16MB
)

const (
	ChunkHeaderSizeFixed = 8 * 1 << 10 // 8KB
	ChunkKeyMaxSize      = 4 * 1 << 10 // 4KB
	ChunkDataSize        = 1 * 1 << 20 // 1MB
)

const BlockSize = 1 << 12

// Vol constants
const (
	MagicBocchi = 0x000b0cc1

	DirDepth = 4

	MaxBucketsPerSegment = 1 << 16 / DirDepth
)
