package main

import (
	"bytes"
	"errors"
	"io"
	"log"
	"net"
)

type Conn struct {
	conn net.Conn
}

//kafka responses are in this format: message_size, header, body
func (c *Conn) HandleConn()(error){
	buff :=  new(bytes.Buffer)
	for {
		buff.Reset()
		data := make([]byte, 100)
		_, err := c.conn.Read(data)
		buff.Write(data)
		if err != nil {
			log.Printf("error while receiving data from conn %v\n",err)
			if err == io.EOF{
				log.Println("EOF")
				return ERR_EOF 
			}
			return err
		}
		response,err := c.parseRequest(buff.Bytes())
		if err != nil {
			//@todo: Improve on the error handling logic
			log.Printf("error while parsing response %v\n", err)
			return err

		}
		err = c.write(&response)
		if err != nil {
			log.Printf("error while writing to the connection: %v\n", err)
			return err
		}
	}
}


func (c *Conn) write(payload *ParseRequest)(error){
	res := NewApiVersionResponse(payload)
	buff,err := res.Encode()
	if err != nil {
		log.Printf("error while encoding response: %v\n", err)
	}

	_, err = c.conn.Write(buff)
	if err != nil {
		log.Printf("error while writing to the connection: %v\n", err)
		return errors.New("err conn write")
	}

	return nil
}


