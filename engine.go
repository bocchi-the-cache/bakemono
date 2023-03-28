package bakemono

import "os"

type Engine struct {
	path string
	fp   *os.File
}

func NewEngine(path string) *Engine {
	return &Engine{
		path: path,
	}
}

func (e *Engine) Init() error {
	fp, err := os.OpenFile(e.path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	e.fp = fp
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
