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
	cursor uint8
	tagBuf byte
}

func NewTopicResponse(req ParsedTopicApiRequest)(TopicResponse, error){
	header := NewTopicResponseHeader(req.correlationId)
	body := NewTopicResponseBody(req.topicArrLen, req.topics)

	tagBuf := uint8(0)

	return TopicResponse{
		msgSize: uint32(0),
		header: header,
		body: body,
		cursor: 0xff,
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

	
	log.Printf("encoded repsone: %x\n", finalBuff.Bytes())

	return finalBuff.Bytes(), nil
}

type topicResponseHeader struct {
	correlationId uint32
	tagBuf byte
	throttleTime uint32
}

type topicPartition struct {
	errorCode int16
	partitionIndex int32
	leaderId int32
	leaderEpoch int32
	replicArrlen uint32
	replicArr []int32
	isrArrLen uint32
	isrArr []int32
	lastKnowELR []int32
	offlineRepl []int32
	tag int8
}

type topicResponseBody struct {
	topicArrLen uint8
	errorCode uint16
	topics []ResponseTopic
	isInternal uint8
	partitionsArr uint8
	topicAuthOps int32
	tagBuf byte
}

func NewTopicResponseBody(topicArrLen uint8, topics []Topic) (topicResponseBody){
	log.Printf("topics: %v\n", topics)
	//here am probably gonna get an error...but it generally does is 

	parsedTopics := make([]ResponseTopic, (topicArrLen))
	if topicArrLen > 0 {
		for i := 0; i < int(topicArrLen); i++ {
			topic := topics[i]
			//this is propably wrong to do since we may get an out of bound error while trying to access the index
			parsedTopics[i] = ResponseTopic{
				len: topic.len,
				contents: topic.name,
				id: [16]byte{},
			} 	
			log.Printf("topic name: %s\n", string(topic.name))
		}
	}

	errCode := []byte{0,3}
	parsedErrCode := binary.BigEndian.Uint16(errCode)


	isInternal := uint8(0)

	partitionsArr :=  uint8(1)
	topicAuthOps := 0

	tagBuf := uint8(0)

	//compact array => +1
	topicArrLen += 1

	return topicResponseBody{
		topicArrLen: topicArrLen,
		isInternal: isInternal,
		partitionsArr: partitionsArr,
		topicAuthOps: int32(topicAuthOps),
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

	for _, topic := range tRB.topics{
		//@todo: In the future move this code to topic struct type
		if err := binary.Write(buff, binary.BigEndian, tRB.errorCode); err != nil {
			log.Printf("error while encoding error code: %v\n", err)
			return nil, errors.New("error while encoding error code")
		}
		topicBytes,err := topic.Encode()
		if err != nil {
			return nil, err
		}
		_, err = buff.Write(topicBytes)
		if err != nil {
			log.Printf("error while writing topic bytes: %v\n", err)
			return nil, err
		}
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
	len uint8
	contents []byte
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
//encodes the topic response header
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


