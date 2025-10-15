package main

import (
	"log"
)

type DynamicBuffer struct {
	data []byte
}

func NewDynamicBuff(cap int) *DynamicBuffer {
	return &DynamicBuffer{
		data: make([]byte, 0,cap),
	}
}

//dynamically inserts bytes in a buff...although for now we are not completly replacing the bytes.Buffer
//if theres a way to dynamically insert elem in the bytes.Buff...please free to reach out and share 
func (buff *DynamicBuffer) Insert(start,end int, data []byte){
	//verification check: checks if the start and end meet the specifications
	if start > end || start < 0 || end > len(buff.data) {
		log.Panicf("invalid range")
	}

	//replaces the range [start:end] with data
	buff.data = append(buff.data[:start], append(data, buff.data[end:]...)...)
}


func (buff *DynamicBuffer) Write(data []byte) {
	buff.data = append(buff.data, data...)
}

func (buff *DynamicBuffer) Bytes() []byte {
	return buff.data
}


//helper function to dynamically allocate a byte buffer already initialized
func ensureCap(buf []byte, minLen int) []byte {
	if len(buf) < minLen {
		//clone the buf but till the elem buf cap
		buf = buf[:cap(buf)]
		if len(buf) < minLen {
			buf = append(buf, make([]byte, minLen - len(buf))...)
		}
	}
	return buf[:minLen]
}
