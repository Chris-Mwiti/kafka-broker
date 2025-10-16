package main

import (
	"encoding/binary"
	"log"
)

//@notes: the function is still under development
type ParseRequest struct {
	RequestMsgSize []byte
	RequestApiKey []byte
	RequestApiVersion []byte
	RequestCorrelationId []byte
}


func (pr *ParseRequest) containsApiVersion()(bool){
	
	if pr.RequestApiVersion == nil {
		return false	
	}
	prVersion := binary.BigEndian.Uint16(pr.RequestApiVersion)
	
	if int(prVersion) >= 0 && int(prVersion) <= 4 {
		return true
	}
	return false
}

func (c *Conn) parseRequest(data []byte)(ParseRequest,error){
	//check that the message size is a 32 bits(signed) 
	
	//payload structure: []byte{message_size+header+body}
	//message_size: 32bits(4 bytes)
	//header: correlationId(4 bytes)
	//body: 

	//so the total bits of data expected is 64bits...the relative data byte array should be [8]byte{}

	//@todo: How do i check that the first 4 bytes of the data are present?
	log.Printf("the following is the data is: %v\n", data)

	if len(data) < 8 {
		log.Printf("error. the received data has a short message size: %v", len(data))
		return ParseRequest{},ERR_MESSAGE_SIZE
	} 


	//request payload
	//@todo: Implment manipulation using byte buffers instead of byte storage itself
	msgSize := data[0:4]
	requestApiKey := data[4:6]
	requestApiVersion := data[6:8]
	correlationId := data[8:]

	log.Printf("the following is the message size: %v\n",msgSize)
	log.Printf("the following is the requestApiKey: %v\n",requestApiKey)
	log.Printf("the following is the requestApiVersion: %v\n", binary.BigEndian.Uint16(requestApiVersion))
	log.Printf("the following is the correlationId: %v\n",correlationId)

	response := ParseRequest{
		RequestMsgSize: msgSize,
		RequestApiKey: requestApiKey,
		RequestApiVersion: requestApiVersion,
		RequestCorrelationId: correlationId,
	}


	return response, nil
}


