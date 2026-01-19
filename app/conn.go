package main

import (
	"bytes"
	"encoding/binary"
	"io"
	"log"
	"net"

	"github.com/boltdb/bolt"
)

type Conn struct {
	conn net.Conn
	db *bolt.DB
}

//@todo: later on in the future modify the function to check multiple version
func checkApiVersion18(payload []byte)(bool, error){
	buff := new(bytes.Buffer)
	buff.Write(payload)
	var msgSize uint32
	if err := binary.Read(buff, binary.BigEndian, &msgSize); err != nil {
		log.Printf("error while reading check version msg size")
		return false, err
	}

	var apiKey uint16
	if err := binary.Read(buff, binary.BigEndian, &apiKey); err != nil {
		log.Printf("error while reading check version api key ")
		return false, err
	}

	log.Printf("api key: %v\n", apiKey)
	 
	if apiKey == 18 {
		return true, nil
	}

	return false, nil
}

//Responsible for handle normal kafka api requests
//kafka responses are in this format: message_size, header, body
//adding some db configurations
func (c *Conn) HandleConn()(error){
	buff :=  new(bytes.Buffer)

	for {
		buff.Reset()

		//@todo: Implement a loader that will load the received data directly to buffer 
		data := make([]byte, 1000)
		_, err := c.conn.Read(data)
		buff.Write(data)
		if err != nil {
			if err == io.EOF {
				log.Println("error from reading from connection: EOF")
			}
			log.Printf("error while receiving data from conn %v\n", err)
			return err
		}

		//check the api key to send request to appropriate handler
		isVersion18, err := checkApiVersion18(data)	
		if err != nil {
			return err
		}

		//@todo: switch statement to handle multiple api version requests
		if isVersion18 {
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
		} else {
			log.Printf("logging a topic request functionality")
			response,err := c.handleTopicRequest(buff.Bytes(), c.db)
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
}


