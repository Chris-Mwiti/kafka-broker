package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"log"
)

type TopicResponse struct {
	header topicResponseHeader
	body topicResponseBody
	cursor uint8
	tagBuf uint8
}

type topicResponseHeader struct {
	msgSize uint32
	correlationId uint32
	tagBuf uint8
	throttleTime uint32
}

type topicResponseBody struct {
	topicArrLen uint16
	errorCode uint16
	topics []ResponseTopic
	isInternal uint8
	partitionsArr uint16
	topicAuthOps uint32
	tagBuf uint8
}

func NewTopicResponseBody(topicArrLen uint16, topics []Topic){
	parsedTopics := make([]ResponseTopic, topicArrLen)
	for i := 0; i < int(topicArrLen); i++ {
		topic := topics[i]
		parsedTopics = append(parsedTopics, ResponseTopic{
			len: topic.len,	
			contents: string(topic.name),
		})
	}

	errCode := []byte{0,3}
	parsedErrCode := binary.BigEndian.Uint16(errCode)

	isInternal := []byte{0}
	parsedIsInternal := uint8(isInternal[0])

	partitionsArr :=  binary.BigEndian.Uint16([]byte{0,1})

}

type ResponseTopic struct {
	len uint16
	contents string
	id [16]byte
}

func NewTopicResponseHeader(msgSize, correlationId uint32)(topicResponseHeader){
	return topicResponseHeader{
		msgSize: msgSize,
		correlationId: correlationId,
		tagBuf: 0,
		throttleTime: 0,
	}
}
func (tRH *topicResponseHeader) Encode()([]byte, error){
	buff := new(bytes.Buffer)

	if err := binary.Write(buff, binary.BigEndian, tRH.correlationId); err != nil {
		log.Printf("error while encoding correlationId: %v\n", err)
		return nil, errors.New("error while encoding correlation id")
	}

	if err := binary.Write(buff, binary.BigEndian, tRH.tagBuf); err != nil {
		log.Printf("error while encoding tag buf: %v\n", err)
		return nil, errors.New("error while encoding tag buf")
	}

	if err := binary.Write(buff, binary.BigEndian, tRH.throttleTime); err != nil {
		log.Printf("error while encoding throttle time: %v\n", err)
		return nil, errors.New("error while encoding throttle time")
	}

	return buff.Bytes(), nil

}


