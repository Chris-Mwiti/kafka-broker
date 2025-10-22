package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"log"
)

type TopicResponse struct {
	msgSize uint32
	header topicResponseHeader
	body topicResponseBody
	cursor int8
	tagBuf uint8
}

func NewTopicResponse(req ParsedTopicApiRequest)(TopicResponse, error){
	header := NewTopicResponseHeader(req.correlationId)
	body := NewTopicResponseBody(req.topicArrLen, req.topics)

	cursor := int8(-1)
	tagBuf := uint8(0)

	return TopicResponse{
		msgSize: uint32(0),
		header: header,
		body: body,
		cursor: cursor,
		tagBuf: tagBuf,
	}, nil
}

func (tr *TopicResponse) Encode() ([]byte, error) {
	buff := new(bytes.Buffer)

	headerBuf, err := tr.header.Encode()
	if err != nil {
		return nil, err
	}
	buff.Write(headerBuf)

	bodyBuf, err := tr.body.Encode()
	if err != nil {
		return nil, err
	}
	if _, err := buff.Write(bodyBuf); err != nil {
		log.Printf("error while writing body buf: %v\n", err)
		return nil, err
	}

	//encode the cursor and tag buff
	if err := binary.Write(buff, binary.BigEndian, tr.cursor); err != nil {
		log.Printf("error while encoding topic response cursor: %v\n", err)
		return nil, errors.New("error while encoding response cursor")
	}

	if err := binary.Write(buff, binary.BigEndian, tr.tagBuf); err != nil {
		log.Printf("error while encoding tag buf: %v\n", err)
		return nil, errors.New("error while encoding tag buf")
	}

	finalBuff := new(bytes.Buffer)
	tr.msgSize = uint32(buff.Len())

	if err := binary.Write(finalBuff, binary.BigEndian, tr.msgSize); err != nil {
		log.Printf("error while encoding msg size: %v\n", err)
		return nil, errors.New("error while encoding msg size")
	}

	if _, err := finalBuff.Write(buff.Bytes()); err != nil {
		log.Printf("error while merging buf to final buf: %v\n", err)
		return nil, errors.New("error while merging buf")
	}

	return finalBuff.Bytes(), nil
}

type topicResponseHeader struct {
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

func NewTopicResponseBody(topicArrLen uint16, topics []Topic) (topicResponseBody){
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


	isInternal := uint8(0)

	partitionsArr :=  binary.BigEndian.Uint16([]byte{0,1})
	topicAuthOps := 0x00000df8

	tagBuf := uint8(0)

	return topicResponseBody{
		topicArrLen: topicArrLen,
		isInternal: isInternal,
		partitionsArr: partitionsArr,
		topicAuthOps: uint32(topicAuthOps),
		tagBuf: tagBuf,
		errorCode: parsedErrCode,
		topics: parsedTopics,
	}
}

func (tRB *topicResponseBody) Encode()([]byte, error){
	buff := new(bytes.Buffer)
	if err := binary.Write(buff,binary.BigEndian, tRB.topicArrLen); err != nil {
		log.Printf("error while encoding topic arr len: %v\n", err)
		return nil, errors.New("error while encoding topic arr len")
	}

	if err := binary.Write(buff, binary.BigEndian, tRB.errorCode); err != nil {
		log.Printf("error while encoding error code: %v\n", err)
		return nil, errors.New("error while encoding error code")
	}


	topicBuff := new(bytes.Buffer)
	for _, topic := range tRB.topics{
		topicBytes,err := topic.Encode()
		if err != nil {
			return nil, err
		}
		_, err = topicBuff.Write(topicBytes)
		if err != nil {
			log.Printf("error while writing topic bytes: %v\n", err)
			return nil, err
		}
	}

	if _,err := buff.Write(topicBuff.Bytes()); err != nil {
		log.Printf("error whiler merging main buf with topic buf: %v\n", err)
		return nil, err
	} 

	if err := binary.Write(buff, binary.BigEndian, tRB.isInternal); err != nil {
		log.Printf("error while writing is internal: %v\n", err)
		return nil, err
	}

	if err := binary.Write(buff, binary.BigEndian, tRB.partitionsArr); err != nil {
		log.Printf("error while writing partitionsArr: %v\n", err)
		return nil, err
	} 

	if err := binary.Write(buff, binary.BigEndian, tRB.topicAuthOps); err != nil {
		log.Printf("error while writing topicAuthOps: %v\n", err)
		return nil, err
	} 

	if err := binary.Write(buff, binary.BigEndian, tRB.tagBuf); err != nil {
		log.Printf("error while writing tagBuf: %v\n", err)
		return nil, err
	} 

	return buff.Bytes(), nil
}

type ResponseTopic struct {
	len uint16
	contents string
	id [16]byte
}

func (rt *ResponseTopic) Encode() ([]byte, error) {
	buff := new(bytes.Buffer)

	if err := binary.Write(buff, binary.BigEndian, rt.len); err != nil {
		log.Printf("error while encoding topic len: %v\n", err)
		return nil, errors.New("error while encoding topic len")
	}

	if err := binary.Write(buff, binary.BigEndian, rt.contents); err != nil {
		log.Printf("error while encoding topic contents: %v\n", err)
		return nil, errors.New("error while encoding contents")
	}

	if err := binary.Write(buff, binary.BigEndian, rt.id); err != nil {
		log.Printf("error while encoding topic id: %v\n", err)
		return nil, errors.New("error while encoding id")
	}
	
	return buff.Bytes(), nil
}

func NewTopicResponseHeader(correlationId uint32)(topicResponseHeader){
	return topicResponseHeader{
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


