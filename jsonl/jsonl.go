package jsonl

import (
	"encoding/json"
	"io"
)

type encoder struct {
	w io.Writer
}

func NewEncoder(w io.Writer) *encoder {
	return &encoder{w: w}
}

func (e *encoder) Encode(v any) error {
	bs, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = e.w.Write(bs)
	if err != nil {
		return err
	}
	_, err = e.w.Write([]byte("\n"))
	if err != nil {
		return err
	}
	return nil
}

func Encode[T any](w io.Writer) func(t T) (T, error) {
	encoder := NewEncoder(w)
	return func(t T) (T, error) {
		return t, encoder.Encode(t)
	}
}
