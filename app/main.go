package main

import (
	"log"
)

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
