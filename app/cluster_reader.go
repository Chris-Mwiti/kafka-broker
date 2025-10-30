package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"log"
	"os"
)

const (
	BASE_CLUSTER_URL = "/tmp/kraft-combined-logs/"
	CLUSTER_METADATA_URL = "__cluster_metadata-0/"
	FILE = "00000000000000000000.log"
)

type RecordBatch struct {
	BaseOffset int64 //offset batch of the record
	BatchLength 	uint32 //lenght of the total items in the batch

	PartitionLeaderEpoch int32 //used to identify the epoch of the leader in this partition
	MagicByte uint8 //used to identify the version of the record bacth
	CRC int32
	Attributes int16 //used to indicate the attributes of the record batch //bitmask format
	LastOffsetDelta int32 //used to indicate the last offset of this record batch and the base offset
	BaseTimeStamp int64 //unix rep of the first batch of record
	MaxTimeStamp int64 //used to represent the max timestamp of the records in the batch
	ProducerId int64 //used to represent the producer id of the batch records

	ProducerEpoch int16 //used to represent the epoch of the producer
	BaseSequence int32 //used to represent the sequence number ordering of the first batch of record
	RecordsLen uint32 //used to represent the 	no of records in the the batch
	Records []Record 
}

type Record struct {
	Length int16 //used to indcate the lenght of the of the record...calc from the attributes till the end
  Attributes int8	
	TimeStampDelta int16 //used to show the offset btn the batch timestamp and the record timestamp
	OffsetDelta int16 //offset btn the batch delta record
	KeyLen int8 //used to indicate the len of the key 
	Key []byte
	ValLen int8
	Val []byte
	HeadersArrCount uint16
}

type RecordVal struct {
	Header ValHeader
}

type ValHeader struct {
	FrameVersion int8
	RecordType int8
	Version int8
}

type FeatureLevelRec struct {
	Header ValHeader
	NameLen uint8 //compact array with a len = len - 1
	Name []byte
	Level int16 //used to indicate the level of the feature
	Tag uint8
}

type TopicLevelRec struct {
	Header ValHeader
	NameLen uint16
	Name []byte
	Id [16]byte
	Tag uint8
}

type PartitionRec struct {
	Header ValHeader
	Id int32
	TopicId [16]byte
	ReplicArrLen uint8
	ReplicArr int32
	SyncReplicArrLen uint8
	SyncReplicaArr int32
	RemoveReplicaLenArr uint8
	AddReplicaArr uint8
	Leader int32
	LeaderEpoch int32
	PartitionEpoch int32
	DirectoriesLen int8
	DirectoryArr [16]byte
	Tag uint8
} 

func readClusterMetaData(path string)(*bytes.Buffer, error){
	if _,err := os.Stat(path); os.IsNotExist(err){
		log.Printf("error while checking file status: %v\n", err)
		return nil, errors.New("file does not exit")	
	} 
	fileBytes, err := os.ReadFile(path)
	if err != nil {
		log.Printf("error while reading file contents: %v\n", err)
		return nil, errors.New("error while reading file contents")
	}

	//create a new buff that will be used to manipulate the retrieved data
	buff := bytes.NewBuffer(fileBytes)

	return buff, nil
}


func processClusterData(buff *bytes.Buffer)([]*RecordBatch, error){
	//first we need to extract the actual length of the bytes
	//from this data we can then use to generate batch records

	batches := make([]*RecordBatch, 0)

	for buff.Len() > 0 {
		batch := RecordBatch{}
		//step 1. Extract the base offset and batch lenght that will be used to generate the record batch arr
		var baseOffset int64
		if err := binary.Read(buff, binary.BigEndian, &baseOffset); err != nil {
			log.Printf("error while reading batch offset: %v\n", err)
			return nil, errors.New("error while extracting batch offset")
		}
		batch.BaseOffset = baseOffset

		var batchLen int32
		if err := binary.Read(buff, binary.BigEndian, &batchLen); err != nil {
			log.Printf("error while reading batch len: %v\n", err)
			return nil, errors.New("error while extracting batch len")
		}
		batch.BatchLength = uint32(batchLen)

		//create a counter that will track the batch bytes to prevent overflow
		actualBatchLen := batchLen
		//if the buff len is less then read the entire buff as a batch
		if buff.Len() < int(actualBatchLen){
			actualBatchLen = int32(buff.Len())
			log.Printf("actual buff len: %v\n", actualBatchLen)
		}

		batchBytes := make([]byte, actualBatchLen)
		if _, err := buff.Read(batchBytes); err != nil {
			log.Printf("error while reading batch bytes: %v\n", err)
			return nil, errors.New("error while extracting batch bytes")
		}
	}
}


func NewBatchReader(buff *bytes.Buffer)(*RecordBatch, error){
	recordBatch := &RecordBatch{}

	var partitionLeaderEpoch int32
	if err := binary.Read(buff, binary.BigEndian, &partitionLeaderEpoch); err != nil {
		log.Printf("error while reading partitionLeaderEpoch: %v\n", err)
		return nil, errors.New("error while reading partition leader epoch")
	}
	recordBatch.PartitionLeaderEpoch = partitionLeaderEpoch

	magicByte, err:= buff.ReadByte()
	if err != nil {
		log.Printf("error while reading magic byte: %v\n", err)
		return nil, errors.New("error while reading magic byte")
	}
	recordBatch.MagicByte = magicByte

	var crc int32
	if err := binary.Read(buff, binary.BigEndian, &crc); err != nil {
		log.Printf("error while reading crc: %v\n", err)
		return nil, errors.New("error while reading crc")
	}
	recordBatch.CRC = crc

	var attr int16
	if err := binary.Read(buff, binary.BigEndian, &attr); err != nil {
		log.Printf("error while reading attr: %v\n", err)
		return nil, errors.New("error while reading attr")
	}
	recordBatch.Attributes = attr

	var lastOffsetDelta int32
	if err := binary.Read(buff, binary.BigEndian, &lastOffsetDelta); err != nil {
		log.Printf("error while reading last offset delta: %v\n", err)
		return nil, errors.New("error while reading last offset delta")
	}
	recordBatch.LastOffsetDelta = lastOffsetDelta

	var baseOffsetTimeStamp int64
	if err := binary.Read(buff, binary.BigEndian, &baseOffsetTimeStamp); err != nil {
		log.Printf("error while reading base offset timestamp: %v\n", err)
		return nil, errors.New("error while reading base offset timestamp")
	}
	recordBatch.BaseTimeStamp = baseOffsetTimeStamp

	var maxTimeStamp int64
	if err := binary.Read(buff, binary.BigEndian, &maxTimeStamp); err != nil {
		log.Printf("error while reading max offset timestamp: %v\n", err)
		return nil, errors.New("error while extracting offset timestamp")
	}
	recordBatch.MaxTimeStamp = maxTimeStamp

	var producerId int64
	if err := binary.Read(buff, binary.BigEndian, &producerId); err != nil {
		log.Printf("error while reading max offset producerId: %v\n", err)
		return nil, errors.New("error while extracting offset producerId")
	}
	recordBatch.ProducerId = producerId

	var producerEpoch int16
	if err := binary.Read(buff, binary.BigEndian, &producerEpoch); err != nil {
		log.Printf("error while reading max offset producerId: %v\n", err)
		return nil, errors.New("error while extracting offset producerId")
	}
	recordBatch.ProducerEpoch = producerEpoch

	var baseSequence int32
	if err := binary.Read(buff, binary.BigEndian, &baseSequence); err != nil {
		log.Printf("error while reading base sequence: %v\n", err)
		return nil, errors.New("error while extracting base sequence")
	}
	recordBatch.BaseSequence = baseSequence

	var recordLen int32
	if err := binary.Read(buff, binary.BigEndian, &recordLen); err != nil {
		log.Printf("error while reading record len: %v\n", err)
		return nil, errors.New("error while extracting record len")
	}
	recordBatch.RecordsLen = uint32(recordLen)

	records := make([]*Record, recordLen)

	for i := 0; i < int(recordLen); i++ {
		record, err := NewRecordReader(buff)
	}
}

func NewRecordReader(buff *bytes.Buffer)(*Record, error){
	record := Record{}
}
