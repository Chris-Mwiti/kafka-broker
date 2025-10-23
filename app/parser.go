package main

import (

	"encoding/binary"
	"log"
)

//@notes: the function is still under development
type ParseRequest struct {
	RequestMsgSize uint32
	RequestApiKey uint16
	RequestApiVersion uint16
	RequestCorrelationId uint32
}


func (pr *ParseRequest) containsApiVersion()(bool){
	
	if int(pr.RequestApiVersion) >= 0 && int(pr.RequestApiVersion) <= 4 {
		return true
	}
	return false
}

func (pr *ParseRequest) ErrorCode()(uint16) {
	if ok := pr.containsApiVersion(); !ok {
		errCode := binary.BigEndian.Uint16([]byte{0,35})
		return errCode
	}

	errCode := binary.BigEndian.Uint16([]byte{0,0})
	return errCode
}

func (c *Conn) handleApiRequest(data []byte)([]byte,error){
	//check that the message size is a 32 bits(signed) 
	
	//payload structure: []byte{message_size+header+body}
	//message_size: 32bits(4 bytes)
	//header: correlationId(4 bytes)
	//body: 

	//so the total bits of data expected is 64bits...the relative data byte array should be [8]byte{}

	//@todo: How do i check that the first 4 bytes of the data are present?
	log.Printf("the following is the data is: %v\n", data)

	if len(data) < 12 {
		log.Printf("error. the received data has a short message size: %v", len(data))
		return nil,ERR_MESSAGE_SIZE
	} 


	//request payload
	//@todo: Implment manipulation using byte buffers instead of byte storage itself
	msgSizeBytes := data[0:4]
	requestApiKeyBytes := data[4:6]
	requestApiVersionBytes := data[6:8]
	correlationIdBytes := data[8:]

	msgSize := binary.BigEndian.Uint32(msgSizeBytes)	
	requestApiKey := binary.BigEndian.Uint16(requestApiKeyBytes)
	requestApiVersion := binary.BigEndian.Uint16(requestApiVersionBytes)
	correlationId := binary.BigEndian.Uint32(correlationIdBytes)

	response := ParseRequest{
		RequestMsgSize: msgSize,
		RequestApiKey: requestApiKey,
		RequestApiVersion: requestApiVersion,
		RequestCorrelationId: correlationId,
	}

	res := NewApiVersionResponse(&response)

	buff,err := res.Encode()
	if err != nil {
		log.Printf("error while encoding response: %v\n", err)
	}

	return buff, nil
}

func (conn *Conn) handleTopicRequest(data []byte)([]byte, error){
	topicReq, err := NewParsedTopicReq(data)
	if err != nil {
		log.Printf("error while parsing topic req: %v\n", err)
		return nil, err
	}

	topicRes, err := NewTopicResponse(*topicReq)
	if err != nil {
		log.Printf("error while creating topic response: %v\n", err)
		return nil,err
	}

	res, err := topicRes.Encode()
	if err != nil {
		log.Printf("error while encoding topic res: %v\n", err)
		return nil, err
	}

	return res, nil
}
