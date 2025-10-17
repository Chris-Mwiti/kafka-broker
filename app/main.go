package main

import (
	"log"
	"sync"
)

func main() {
	conn := Conn{
		address: "0.0.0.0:9092",
		protocol: "tcp",
	}

	//create a listening go routine
	var wg sync.WaitGroup

	wg.Add(1)
	go func(){
		defer wg.Done()
		err := conn.Listen()
		if err != nil {
			log.Panicf("error while listening to connection status: %v\n", err)
		}
	}()

	log.Println("waiting for all connections to be completed")
	wg.Wait()
}
