package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"unicode/utf8"
)

type ParsedTopicApiRequest struct {
	msgSize uint32
	apiKey uint16
	apiVersion uint16
	correlationId uint32
	client Client
	topicArrLen uint8
	topics []Topic
	responsePartitionLimit uint32
	cursor uint8
	tagBuf byte
}

type Client struct {
	clientIdLen uint16
	clientId string
	tagBuf byte
}

type Topic struct {
	len uint8 
	name []byte
	tagBuf byte
}

func NewParsedTopicReq(payload []byte)(*ParsedTopicApiRequest, error){
	reader := bytes.NewReader(payload)

	var msgSize uint32
	if err := binary.Read(reader, binary.BigEndian, &msgSize); err != nil {
		log.Printf("error while parsing message size: %v\n", err)
		return nil, errors.New("error while parsing message size")
	}

	var apiKey uint16
	if err := binary.Read(reader, binary.BigEndian, &apiKey); err != nil {
		log.Printf("error while parsing api key: %v\n",err)
		return nil, errors.New("error while parsing api key")
	}

	var apiVersion uint16
	if err := binary.Read(reader, binary.BigEndian, &apiVersion); err != nil {
		log.Printf("error while parsing api version: %v\n", err)
		return nil, errors.New("error while parsing api version")
	}

	var correlationId uint32
	if err := binary.Read(reader, binary.BigEndian, &correlationId); err != nil {
		log.Printf("errors while parsing correlation id: %v\n",err)
		return nil, errors.New("error while parsing correlation id")
	}
	
	log.Printf("correlation id: %v\n", correlationId)

	var clientIdLen uint16
	if err := binary.Read(reader, binary.BigEndian, &clientIdLen); err != nil {
		log.Printf("error while parsing client id: %v\n", err)
		return nil, errors.New("error while parsing client id len")
	}
	clientId := make([]byte, clientIdLen)
	if _, err := reader.Read(clientId); err != nil {
		log.Printf("error while reading client id: %v\n", err)
		return nil, errors.New("error while parsing client id")
	}
	if !utf8.Valid(clientId) {
		return nil, errors.New("invalid client id")
	}

	log.Printf("client id: %v\n", string(clientId))

	var clientTagBuf byte
	if err := binary.Read(reader, binary.BigEndian, &clientTagBuf); err != nil {
		log.Printf("error while reading client tag buf: %v\n", err)
		return nil, errors.New("error while parsing client tag buf")
	}

	log.Printf("client tag buf: %d", clientTagBuf)

	var topicsArrLen uint8
	if err := binary.Read(reader, binary.BigEndian, &topicsArrLen); err != nil {
		log.Printf("error while reading topic array len: %v\n", err)
		return nil, errors.New("error while parsing topics arr len")
	}

	log.Printf("topic arr len: %d\n", topicsArrLen)
	
	topics := make([]Topic, int(topicsArrLen))
	for i := 0; i < int(topicsArrLen - 1); i++ {
		var topicLen uint8
		if err := binary.Read(reader, binary.BigEndian, &topicLen); err != nil {
			log.Printf("error while reaeding topic len: %v\n", err)
			return nil, errors.New("error while parsing topic name len")
		}

		if reader.Len() < int(topicsArrLen) + 1 {
			log.Printf("not enough bytes for topic: %d\n", i)
			return nil, fmt.Errorf("error not enough bytes for topic: %d\n", i)
		}
		topic := make([]byte, topicLen)
		if _, err := reader.Read(topic); err != nil {
			log.Printf("error while reading topic name: %v\n", err)
			return nil, errors.New("error while parsing topic name")
		}
		var tagBuf byte
		if err := binary.Read(reader, binary.BigEndian, &tagBuf); err != nil {
			log.Printf("error while reading topic tag buf: %v\n",err)
			return nil, errors.New("error while parsing topic tag buf")
		}
		topics[i] = Topic{len: topicLen, name: topic, tagBuf: tagBuf}
	}


	var responsePartLimit uint32
	if err := binary.Read(reader, binary.BigEndian, &responsePartLimit); err != nil {
		log.Printf("error while reading responsePartLimit: %v\n", err)
		return nil, errors.New("error while parsing response partion limit")
	}

	var cursor byte
	if err := binary.Read(reader, binary.BigEndian, &cursor); err != nil {
		log.Printf("error while reading curson: %v\n", err)
		return nil, errors.New("error while parsing cursor")
	}

	var tagBuf uint8
	if err := binary.Read(reader, binary.BigEndian, &tagBuf); err != nil {
		log.Printf("error while reading tag buf: %v\n", err)
		return nil, errors.New("error while parising tag buf")
	}

	return &ParsedTopicApiRequest{
		msgSize: msgSize,
		apiKey: apiKey,
		apiVersion: apiVersion,
		correlationId: correlationId,
		client: Client{
			clientIdLen: clientIdLen,
			clientId: string(clientId),
			tagBuf: clientTagBuf,
		},
		topicArrLen: topicsArrLen,
		topics: topics,
		responsePartitionLimit: responsePartLimit,
		cursor: cursor,
		tagBuf: tagBuf,
	}, nil
}
