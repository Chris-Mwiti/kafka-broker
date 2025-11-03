package main

import "bytes"

//used to write unsigned variables
func WriteVariant(buff *bytes.Buffer, value uint64) error {
	for value >= 0x80 {
		buff.WriteByte(byte(value) | 0x80)
		value >>= 7
	}

	buff.WriteByte(byte(value)) //most significant byte end
	return nil
}

//used to write signed varibles
func WriteZigZag(buff *bytes.Buffer, value int64) error {
	unsigned := uint64((value << 1) ^ (value >> 63))
	return WriteVariant(buff, unsigned)
}
