package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"log"
	"net"
)

type Conn struct {
	protocol string
	address string
	conn net.Conn
}

func NewConn(proto,addr string)(*Conn){
	return &Conn{
		protocol: proto,
		address: addr,
	}
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
func (c *Conn) read()(*ParseRequest,error){
	
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

	response,err := c.parseRequest(buff.Bytes())
	if err != nil {
		//@todo: Improve on the error handling logic
		log.Printf("error while parsing response %v\n", err)
	}
	buff.Reset()
	return &response, nil	
}


func (c *Conn) write(payload *ParseRequest)(error){
	//possibly the capacity will change
	resp := make([]byte, 0)
	resp = ensureCap(resp,10)

	//at the end eventually set the size of the array
	//@note: the msg size should not count itself
	//size := len(resp)
	//@notes: actually recommended tp be set at 14
	sb := []byte{0,0,0,19}
	uSb := binary.BigEndian.Uint32(sb)
	binary.BigEndian.PutUint32(resp[0:4], uSb)


	
	//convert the payload structure to fit the BigEndian format
	u32CorrelationId := binary.BigEndian.Uint32(payload.RequestCorrelationId)
	binary.BigEndian.PutUint32(resp[4:8], u32CorrelationId)

	//@notes: the request api version is a signed 16 bit integer
	//@notes: various api requests which is identified by the request_api_key
	//@notes: the api requests can support various api versions range
	if ok := payload.containsApiVersion(); !ok {
		errCode := []byte{0,35}
		binary.BigEndian.PutUint16(resp[8:10], binary.BigEndian.Uint16(errCode))
	} else {
		errCode := []byte{0,0}
		binary.BigEndian.PutUint16(resp[8:10], binary.BigEndian.Uint16(errCode))
	}
	
	//the format of the response expected is as follows:
	//msgSize -> correlationId -> errCode -> array-content-lenght -> api_key -> minV -> maxV

	//set the response array content len
	resp = ensureCap(resp, 17)
	resp[10] = 2

	//setting of the api key
	apiKey := binary.BigEndian.Uint16(payload.RequestApiKey)
	binary.BigEndian.PutUint16(resp[11:13],apiKey)

	//setting the versions of the kafka headers
	minV := binary.BigEndian.Uint16([]byte{0,0})
	maxV := binary.BigEndian.Uint16([]byte{0,4})

	binary.BigEndian.AppendUint16(resp,minV)
	binary.BigEndian.AppendUint16(resp,maxV)

	resp = ensureCap(resp, 23)

	//tag buffer
	resp[17] = 0
	binary.BigEndian.AppendUint32(resp,binary.BigEndian.Uint32([]byte{0,0,0,0}))

		_, err := c.conn.Write(resp)
	if err != nil {
		log.Printf("error while writing to the connection: %v\n", err)
		return errors.New("err conn write")
	}

	return nil
}


