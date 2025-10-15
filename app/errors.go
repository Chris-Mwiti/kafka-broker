package main

import (
	"errors"
)

//err formats
var ERR_READ_CONN = errors.New("error while reading from the connection")
var ERR_PARSE_CONN = errors.New("error while parsing the connection data")
var ERR_MESSAGE_SIZE = errors.New("error the payload message size is not met")
var ERR_EOF = errors.New("EOF")


