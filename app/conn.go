package main

import (
	"bytes"
	"io"
	"log"
	"net"
)

type Conn struct {
	conn net.Conn
}

//Responsible for handle normal kafka api requests
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
		response,err := c.handleApiRequest(buff.Bytes())
		if err != nil {
			//@todo: Improve on the error handling logic
			log.Printf("error while parsing response %v\n", err)
			return err

		}
		_,err = c.conn.Write(response)
		if err != nil {
			log.Printf("error while writing to the connection: %v\n", err)
			return err
		}
	}
}

//Responsible for handling kafka topic request
func (c *Conn) HandleTopicConn()(error){
	buff := new(bytes.Buffer)

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
		response,err := c.handleTopicRequest(buff.Bytes())
		log.Printf("handl topic request: %x\n", response)
		if err != nil {
			//@todo: Improve on the error handling logic
			log.Printf("error while parsing response %v\n", err)
			return err

		}
		_, err = c.conn.Write(response)
		if err != nil {
			log.Printf("error while writing to the connection: %v\n", err)
			return err
		}
	}
}





