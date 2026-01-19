package main

import (
	"log"
	"net"

	"github.com/boltdb/bolt"
)

type Server struct {
	proto string
	addr string
	listener net.Listener
}

func NewServer(proto,addr string)(*Server, error){
	l, err := net.Listen(proto, addr)
	if err != nil {
		return nil, err
	}
	return &Server{proto: proto, addr: addr, listener: l}, nil
}

func (s *Server) Listen() error {
	log.Printf("Server listening on %s://%s", s.proto, s.addr)

	//create a new database connection
	db, err := bolt.Open(".././db/cluster.db", 0766, bolt.DefaultOptions)
	if err != nil {
		log.Printf("error while establishing database connection: %v\n", err)
		return err
	}
	//load the cluster metadata file
  err = ReadClusterFile(db)

	if err != nil {
		log.Printf("error while processing cluster file: %v\n", err)
		return nil
	}



	for {
		clientConn, err := s.listener.Accept()
		if err != nil {
			log.Printf("error accepting: %v", err)
			continue
		}

		go func(conn net.Conn){

			defer conn.Close()

			clientHandler := &Conn{conn: conn, db: db} 
			if err := clientHandler.HandleConn(); err != nil {
				log.Printf("Client handler error: %v", err)
			}
		}(clientConn)
	}
}


func main() {
	server, err := NewServer("tcp", "0.0.0.0:9092")


	if err != nil {
		log.Panicf("error while starting up server %v\n", err)
	}
	//create a listening go routine
	err = server.Listen()
	if err != nil {
		log.Panicf("error while listening to connection status: %v\n", err)
	}

}
