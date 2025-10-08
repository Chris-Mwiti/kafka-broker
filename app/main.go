package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"log"
	"net"
	"os"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

//err formats
var ERR_READ_CONN = errors.New("error while reading from the connection")
var ERR_PARSE_CONN = errors.New("error while parsing the connection data")
var ERR_MESSAGE_SIZE = errors.New("error the payload message size is not met")

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

	conn, err := l.Accept()
	if err != nil {
		log.Printf("error while accepting connections %v\n", err)
		return err
	}

	//set the conn the the conn instance
	c.conn = conn
	return nil
}

//kafka responses are in this format: message_size, header, body
func (c *Conn) Read(data []byte)([]byte,error){
	for {
		_, err := c.conn.Read(data)
		if err != nil {
			log.Printf("error while receiving data from conn %v\n",err)
			if err == io.EOF{
				log.Println("EOF")
				break
			}
			return nil,err
		}
	}
	
	return data, nil	
}

func (c *Conn) parseResponse(data []byte)(error){
	//check that the message size is a 32 bits(signed) 
	
	//payload structure: []byte{message_size+header+body}
	//message_size: 32bits(4 bytes)
	//header: correlationId(4 bytes)
	//body: 

	//so the total bits of data expected is 64bits...the relative data byte array should be [8]byte{}

	//@todo: How do i check that the first 4 bytes of the data are present?

	if len(data) < 8 {
		log.Printf("error. the received data has a short message size: %v", len(data))
		return ERR_MESSAGE_SIZE
	} 

	size := binary.BigEndian.Uint32(data[:4])
	if int(size) != len(data){
		log.Printf("the message size payload is short")
		return ERR_MESSAGE_SIZE
	}
	
	correlationId := data[4:8]
	expectedCorrlId := [4]byte{7}

	if ok := bytes.Equal(expectedCorrlId[:], correlationId); !ok{
		log.Printf("expected correlation id not met\n")	
		return ERR_PARSE_CONN	
	}


	return nil
}

func main() {
	conn := Conn{
		address: "0.0.0.0:9092",
		protocol: "tcp",
	}
	err := conn.Listen()
	if err != nil {
		log.Printf("error while listening to the connection %v\n", err)
		os.Exit(1)
	}
	defer func (){
		err := conn.conn.Close()
		if err != nil {
			log.Panicf("error while closing tcp connections %v\n", err)
		}
	}()

	buff := bytes.Buffer{}
	data := make([]byte,1024)
	go func(){
		_, err := conn.Read(data)
		if err != nil {
			log.Panicf("error while reading from the connection %v\n", err)
		}
		buff.Write(data)
	}()

	err = conn.parseResponse(buff.Bytes())
	if err != nil {
		log.Panicf("error while parsing response")
	}

	//flash down the first 4 bytes before accessing the correlation id
	curr := buff.Next(4)
	//conver the the bytes format to BigEndian format
	msgSize := binary.BigEndian.Uint32(curr)
	log.Printf("message size received %v\n", msgSize)

	//header 
	correlationId := buff.Next(4)
	headerSize := binary.BigEndian.Uint32(correlationId)
	
	
	respBytes := make([]byte, 1024)
	resp := binary.BigEndian.AppendUint32(respBytes,headerSize)
	log.Printf("correlation Id is %v\n header size %v\n", correlationId, headerSize)

	conn.conn.Write(resp)
}
