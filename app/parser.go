package main

import (
	"bytes"
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
	buff := new(bytes.Buffer)
	
	//payload structure: []byte{message_size+header+body}
	//message_size: 32bits(4 bytes)
	//header: correlationId(4 bytes)
	//body: 
	log.Printf("the following is the data is: %v\n", data)

	if len(data) < 12 {
		log.Printf("error. the received data has a short message size: %v", len(data))
		return nil,ERR_MESSAGE_SIZE
	} 
	
	var msgSize uint32 
	if err := binary.Read(buff, binary.BigEndian, &msgSize); err != nil {
		log.Printf("error while reading message size")
		return nil, err
	}

	var reqApiKey uint16 
	if err := binary.Read(buff, binary.BigEndian, &reqApiKey); err != nil {
		log.Printf("error while readint request api key")
		return nil, err
	}

	var reqApiVersion uint16
	if err := binary.Read(buff, binary.BigEndian, &reqApiVersion); err != nil {
		log.Printf("error while reading request api version")
		return nil, err
	}
	
	var correlationId uint32
	if err := binary.Read(buff, binary.BigEndian, &correlationId); err != nil {
		log.Printf("error while reading correlation id")
		return nil, err
	}


	request := ParseRequest{
		RequestMsgSize: msgSize,
		RequestApiKey: reqApiKey,
		RequestApiVersion: reqApiVersion,
		RequestCorrelationId: correlationId,
	}

	res := NewApiVersionResponse(&request)

	buf,err := res.Encode()
	if err != nil {
		log.Printf("error while encoding response: %v\n", err)
	}

	return buf, nil
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
