package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"log"
	"net"
	"os"
	"strconv"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

//err formats
var ERR_READ_CONN = errors.New("error while reading from the connection")
var ERR_PARSE_CONN = errors.New("error while parsing the connection data")
var ERR_MESSAGE_SIZE = errors.New("error the payload message size is not met")
var ERR_EOF = errors.New("EOF")

type Conn struct {
	protocol string
	address string
	conn net.Conn
}

func (c *Conn) Listen() (error){
	l, err := net.Listen(c.protocol, c.address)
	if err != nil {
		log.Printf("error while binding to port %s: %v\n", c.address, err)
		return err
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Printf("error while accepting connections %v\n", err)
			return err
		}
		//set the conn the the conn instance
		c.conn = conn

		go func(){
			msg, err := c.read()
			if err != nil {
				//@todo: Think a way around this especially in terms of err groups
				log.Printf("error while reading connection %v\n", err)
			}
			log.Printf("received msg: %v\n", msg)
			err = c.write(msg)
			if err != nil {
				log.Printf("error while writing to connection %v\n", err)
			}
		}()
	}

}

//kafka responses are in this format: message_size, header, body
func (c *Conn) read()(*ParseResponse,error){
	
	data := make([]byte, 4096)
	var buff bytes.Buffer
	_, err := c.conn.Read(data)
	buff.Write(data)
	if err != nil {
		log.Printf("error while receiving data from conn %v\n",err)
		if err == io.EOF{
			log.Println("EOF")
			return nil, ERR_EOF 
		}
		return nil,err
	}

	response,err := c.parseResponse(buff.Bytes())
	if err != nil {
		//@todo: Improve on the error handling logic
		log.Printf("error while parsing response %v\n", err)
	}
	buff.Reset()
	return &response, nil	
}

func (c *Conn) write(payload *ParseResponse)(error){
	//possibly the capacity will change
	resp := make([]byte, 24)
	//@todo: dynamic assignment of msgsize bytes size
	binary.BigEndian.PutUint32(resp[0:4], 0)

	//convert the payload structure to fit the BigEndian format
	u32CorrelationId := binary.BigEndian.Uint32(payload.RequestCorrelationId)
	binary.BigEndian.PutUint32(resp[4:8], u32CorrelationId)

	//@notes: the request api version is a signed 16 bit integer
	//@notes: various api requests which is identified by the request_api_key
	//@notes: the api requests can support various api versions range
	if ok := payload.containsApiVersion(); !ok{
		errCode := binary.BigEndian.Uint16([]byte{0,35})
		binary.BigEndian.PutUint16(resp[8:10],errCode)
	} else {
		errCode := binary.BigEndian.Uint16([]byte{0,0})
		binary.BigEndian.PutUint16(resp[8:10],errCode)
	}
	_, err := c.conn.Write(resp)
	if err != nil {
		log.Printf("error while writing to the connection: %v\n", err)
		return errors.New("err conn write")
	}

	return nil
}

//@notes: the function is still under development
type ParseResponse struct {
	RequestMsgSize []byte
	RequestApiKey []byte
	RequestApiVersion []byte
	RequestCorrelationId []byte
}

func (pr *ParseResponse) containsApiVersion()(bool){
	
	latestApiVersions := []string{"0", "1", "2", "3", "4"}
	if pr.RequestApiVersion != nil {
		return false	
	}
	prVersion := binary.BigEndian.Uint16(pr.RequestApiVersion)
	for _,version := range latestApiVersions{

		if v,_ := strconv.Atoi(version); int(prVersion) == v {
			return true
		}

	}
	return false
}
func (c *Conn) parseResponse(data []byte)(ParseResponse,error){
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
		return ParseResponse{},ERR_MESSAGE_SIZE
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

	response := ParseResponse{
		RequestMsgSize: msgSize,
		RequestApiKey: requestApiKey,
		RequestApiVersion: requestApiVersion,
		RequestCorrelationId: correlationId,
	}


	return response, nil
}

func main() {
	conn := Conn{
		address: "0.0.0.0:9092",
		protocol: "tcp",
	}

	//create a listening go routine
	err := conn.Listen()
	if err != nil {
		log.Panicf("error while listening to connection status: %v\n", err)
	}

}
