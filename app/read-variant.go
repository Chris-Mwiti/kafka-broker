package main

import (
	"bytes"
	"errors"
)

//responsible for dealing with unsigned variable instances
func ReadVariant(buff *bytes.Buffer) (uint32, error) {
	var result uint32
	var shift uint

	for {
		if buff.Len() == 0 {
			return 0, errors.New("buffer underflow while reading variang")
		}
		b, err := buff.ReadByte()
		if err != nil {
			return 0, err
		}
		result |= uint32(b&0x7f) << uint32(shift)

		if b&0x80 == 0 {
			break //here the most significant bit is 0
		}

		shift += 7
		if shift >= 35 {
			return 0, errors.New("variant overflow")
		}
	}
	return result, nil
}

//responsible for reading the signed variable instance
func ReadZigZag(buff *bytes.Buffer)(int64, error){
	u, err := ReadVariant(buff)

	if err != nil {
		return 0, err
	}

	return int64(u>>1) ^ -int64(u&1), nil
}
