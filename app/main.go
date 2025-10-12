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
			err = c.write()
			if err != nil {
				log.Printf("error while writing to connection %v\n", err)
			}
		}()
	}

}

//kafka responses are in this format: message_size, header, body
func (c *Conn) read()([]byte,error){
	
	data := make([]byte, 4096)
	var buff bytes.Buffer
	_, err := c.conn.Read(data)
	buff.Write(data)
	if err != nil {
		log.Printf("error while receiving data from conn %v\n",err)
		if err == io.EOF{
			log.Println("EOF")
			return data, nil
		}
		return nil,err
	}

	err = c.parseResponse(buff.Bytes())
	if err != nil {
		//@todo: Improve on the error handling logic
		log.Printf("error while parsing response %v\n", err)
	}
	buff.Reset()
	return data, nil	
}

func (c *Conn) write()(error){
	//possibly the capacity will change
	resp := make([]byte, 8)
	binary.BigEndian.PutUint32(resp[0:4], 0)
	binary.BigEndian.PutUint32(resp[4:8], 7)

	_, err := c.conn.Write(resp)
	if err != nil {
		log.Printf("error while writing to the connection: %v\n", err)
		return errors.New("err conn write")
	}

	return nil
}

func (c *Conn) parseResponse(data []byte)(error){
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
		return ERR_MESSAGE_SIZE
	} 

	correlationId := data[4:8]
	expectedCorrlId := [4]byte{7}

	if ok := bytes.Equal(expectedCorrlId[:], correlationId); !ok{
		log.Printf("expected correlation id not met correlationId %v\n", expectedCorrlId)	
		return ERR_PARSE_CONN	
	}


	return nil
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
