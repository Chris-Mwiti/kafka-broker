package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"log"

	"github.com/boltdb/bolt"
)

type TopicResponse struct {
	msgSize uint32
	header topicResponseHeader
	body topicResponseBody
	cursor int8
	tagBuf byte
}

func NewTopicResponse(req ParsedTopicApiRequest, db *bolt.DB)(TopicResponse, error){
	header := NewTopicResponseHeader(req.correlationId)
	body, err := NewTopicResponseBody(req.topicArrLen, req.topics, db)
	
	if err != nil {
		log.Printf("error while creating topic response body: %v\n", err)
		return TopicResponse{}, err
	}

	tagBuf := uint8(0)

	return TopicResponse{
		msgSize: uint32(0),
		header: header,
		body: *body,
		cursor: -1,
		tagBuf: tagBuf,
	}, nil
}

func (tr *TopicResponse) Encode() ([]byte, error) {
	buff := new(bytes.Buffer)

	headerBuf, err := tr.header.Encode()
	if err != nil {
		return nil, err
	}
	
	if _, err := buff.Write(headerBuf); err != nil {
		log.Printf("error while writing topic header response: %v", err)
		return nil, err
	}

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

	//setting the msgSize field
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

func (tp *topicPartition) Encode() ([]byte, error){
	buff := new(bytes.Buffer)

	if err := binary.Write(buff, binary.BigEndian, tp.errorCode); err != nil {
		log.Printf("error while writing partition error code: %v\n", err)
		return nil, err
	}

	if err := binary.Write(buff, binary.BigEndian, tp.partitionIndex); err != nil {
		log.Printf("errro while writing partition index: %v\n", err)
		return nil, err
	}

	if err := binary.Write(buff, binary.BigEndian, tp.leaderId); err != nil {
		log.Printf("error while writing partition leader id: %v\n", err)
		return nil, err
	}

	if err := binary.Write(buff, binary.BigEndian, tp.leaderEpoch); err != nil {
		log.Printf("error while writing leader epoch: %v\n", err)
		return nil, err
	}

	tp.replicArrlen++
	if err := binary.Write(buff, binary.BigEndian, tp.replicArrlen); err != nil {
		log.Printf("error while writing replicaArr len: %v\n", err)
		return nil, err
	}

	if err := binary.Write(buff, binary.BigEndian, tp.replicArr); err != nil {
		log.Printf("error while writing replicaArr: %v\n", err)
		return nil, err
	}

	tp.isrArrLen++
	if err := binary.Write(buff, binary.BigEndian, tp.isrArrLen); err != nil {
		log.Printf("error while writing isrArrLen: %v\n", err)
		return nil, err
	}

	if err := binary.Write(buff, binary.BigEndian, tp.isrArr); err != nil {
		log.Printf("error while writing isrArr: %v\n", err)
		return nil, err
	}

	//there might be an error in this section....can't find the elr from the source partition
	if err := binary.Write(buff, binary.BigEndian, int32(len(tp.lastKnowELR))); err != nil {
		log.Printf("error while writing last known elr: %v\n", err)
		return nil, err
	}

	if err := binary.Write(buff, binary.BigEndian, int32(len(tp.lastKnowELR))); err != nil {
		log.Printf("error while writing last know elr: %v\n", err)
		return nil, err
	}

	if err := binary.Write(buff, binary.BigEndian, int32(len(tp.offlineRepl))); err != nil {
		log.Printf("error while writing offline leader replica: %v\n", err)
		return nil, err
	}


	if err := binary.Write(buff, binary.BigEndian, tp.tag); err != nil {
		log.Printf("error while writing tag buff: %v\n", err)
		return nil, err
	}

	return buff.Bytes(), nil
}

//@todo: Later on in the future the loaded topic partitions should be temporarily stored in a in mem db.
func NewTopicPartitions(topicId [16]byte, db *bolt.DB)([]topicPartition, error){

	partitions, err := GetItemsPartitionBucket(db, string(topicId[:]))
	if err != nil {
		log.Printf("error while getting items from partition: %v\n", err)
		return nil, err
	}

	topicPartitions := make([]topicPartition, 0)
	for _, partition := range *partitions {
		topicPartition := new(topicPartition)

		topicPartition.errorCode = 0
		topicPartition.tag = 0
		topicPartition.partitionIndex = partition.Id
		topicPartition.leaderId = partition.Leader
		topicPartition.leaderEpoch = partition.LeaderEpoch
		topicPartition.replicArrlen = partition.ReplicArrLen
    topicPartition.replicArr = partition.ReplicArr
		topicPartition.isrArrLen = partition.SyncReplicArrLen
		topicPartition.isrArr = partition.SyncReplicaArr

		topicPartitions = append(topicPartitions, *topicPartition)
	}
	
	return topicPartitions, nil
}

type topicResponseBody struct {
	topicArrLen uint8
	topics []ResponseTopic
}

func NewTopicResponseBody(topicArrLen uint8, topics []Topic, db *bolt.DB) (*topicResponseBody, error){
	log.Printf("topics: %v\n", topics)

	//here am probably gonna get an error...but it generally does is 
	parsedTopics := make([]ResponseTopic, (topicArrLen))
	if topicArrLen > 0 {
		for i := 0; i < int(topicArrLen); i++ {
			topic := topics[i]
			storedTopic, err := GetItemTopicBucket(db, string(topic.name)) 

			if err != nil {
				log.Printf("topic not found: %v\n", err)
				return nil, errors.Join(errors.New("topic not found"), err)
			}


			isInternal := uint8(0)
			topicAuthOps := int32(0)
			tagBuf := uint8(0)
			partitionsArr, err := NewTopicPartitions(storedTopic.Id, db)
			if err != nil {
				return nil, err
			}

			partitionsArrlen := len(partitionsArr) + 1

			//this is propably wrong to do since we may get an out of bound error while trying to access the index
			parsedTopics[i] = ResponseTopic{
				len: topic.len,
				contents: topic.name,
				id: storedTopic.Id,
				errorCode: int16(0),
				isInternal: isInternal,
				partitionsArr: partitionsArr,
				partitionsArrLen: uint32(partitionsArrlen),
				topicAuthOps: topicAuthOps,
				tagBuf: tagBuf,
			} 	
			log.Printf("topic name: %s\n", string(topic.name))
		}
	}

	//compact array => +1
	topicArrLen += 1

	return &topicResponseBody{
		topicArrLen: topicArrLen,
		topics: parsedTopics,
	}, nil
}

func (tRB *topicResponseBody) Encode()([]byte, error){
	buff := new(bytes.Buffer)
	if err := binary.Write(buff,binary.BigEndian, tRB.topicArrLen); err != nil {
		log.Printf("error while encoding topic arr len: %v\n", err)
		return nil, errors.New("error while encoding topic arr len")
	}

	for _, topic := range tRB.topics{
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

	return buff.Bytes(), nil
}

type ResponseTopic struct {
	errorCode int16
	len uint8
	contents []byte
	id [16]byte
	partitionsArrLen uint32
	partitionsArr []topicPartition
	isInternal uint8
	topicAuthOps int32
	tagBuf byte
}

func (rt *ResponseTopic) Encode() ([]byte, error) {
	buff := new(bytes.Buffer)
	if err := binary.Write(buff, binary.BigEndian, rt.errorCode); err != nil {
		log.Printf("error while encoding error code: %v\n", err)
		return nil, errors.New("error while encoding error code")
	}

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

	if err := binary.Write(buff, binary.BigEndian, rt.partitionsArrLen); err != nil {
		log.Printf("error while encoding the partitions arr len: %v\n", err)
		return nil, errors.New("error while encoding arr partitions len")
	}

	partitionArrBuff := new(bytes.Buffer)

	for _, partition := range rt.partitionsArr {
		data, err := partition.Encode()
		if err != nil {
			log.Printf("error while encoding partition")
			return nil, err
		}
		partitionArrBuff.Write(data)
	}

	if _, err := buff.Write(partitionArrBuff.Bytes()); err != nil {
		log.Printf("error while writing partition arr buff: %v\n", err)
		return nil, errors.New("error while encoding partition arr buff")
	}
	

	if err := binary.Write(buff, binary.BigEndian, rt.topicAuthOps); err != nil {
		log.Printf("error while encoding topic authorized ops: %v\n", err)
		return nil, errors.New("error while encoding topic authorized ops")
	}

	if err := binary.Write(buff, binary.BigEndian, rt.tagBuf); err != nil {
		log.Printf("error while encoding tag buf: %v\n", err)
		return nil, errors.New("error while encoding tag buf")
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


