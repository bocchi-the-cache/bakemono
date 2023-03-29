package bakemono

import "os"

const BlockSize = 1 << 12

type Engine struct {
	path        string
	SizeMb      uint32
	SliceSizeKb uint32
	fp          *os.File

	Volume *Vol
}

func NewEngine(cfg *EngineConfig) *Engine {
	return &Engine{
		path:        cfg.Path,
		SizeMb:      cfg.SizeMb,
		SliceSizeKb: cfg.SliceSizeKb,
	}
}

func (e *Engine) Init() error {
	//var fileSize uint64
	//fileSize = uint64(e.SizeMb) * 1024 * 1024
	//blocks := fileSize / BlockSize
	//fileUse := blocks * BlockSize

	fp, err := os.OpenFile(e.path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	e.fp = fp

	err = e.parseVol()

	return nil
}

func (e *Engine) parseVol() error {
	e.Volume = &Vol{
		Path: e.path,
		Fp:   e.fp,
	}
	return nil
}

func (e *Engine) Set(key, value []byte) error {
	return nil
}

func (e *Engine) Get(key []byte) ([]byte, error) {
	return nil, nil
}

func (e *Engine) Delete(key []byte) error {
	return nil
}
