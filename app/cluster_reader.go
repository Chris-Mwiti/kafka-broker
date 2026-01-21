package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"log"
	"os"

	"github.com/boltdb/bolt"
	"github.com/google/uuid"
)

const (
	BASE_CLUSTER_URL = "/tmp/kraft-combined-logs/"
	CLUSTER_METADATA_URL = "__cluster_metadata-0/"
	FILE = "00000000000000000000.log"
	PATH = BASE_CLUSTER_URL + CLUSTER_METADATA_URL + FILE
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
	Length int64 //used to indcate the lenght of the of the record...calc from the attributes till the end
  Attributes int8	
	TimeStampDelta int64 //used to show the offset btn the batch timestamp and the record timestamp
	OffsetDelta int64 //offset btn the batch delta record
	KeyLen int64 //used jto indicate the len of the key 
	Key []byte
	ValLen int64
	Val []byte
	HeadersArrCount uint8
}

type RecordVal struct {
	Header ValHeader
}

type ValHeader struct {
	FrameVersion int8
	RecordType int8
	Version int8
	db *bolt.DB
}


type FeatureLevelRec struct {
	Header ValHeader
	NameLen uint32 //compact array with a len = len - 1
	Name []byte
	Level int16 //used to indicate the level of the feature
	Tag int64
}
var FeatureLevelMap = make(map[string]FeatureLevelRec)

type TopicLevelRec struct {
	Header ValHeader
	NameLen uint32
	Name []byte
	Id [16]byte
	Tag int64
}
var TopicLevelMap = make(map[string]TopicLevelRec)

type PartitionRec struct {
	Header ValHeader
	Id int32
	TopicId [16]byte
	ReplicArrLen uint32
	ReplicArr []int32
	SyncReplicArrLen uint32
	SyncReplicaArr []int32
	RemoveReplicaLenArr uint32
	AddReplicaArr uint32
	Leader int32
	LeaderEpoch int32
	PartitionEpoch int32
	DirectoriesLen uint32
	DirectoryArr [][]byte
	Tag int64
} 
var TopicPartiotionsMap = make(map[uuid.UUID][]PartitionRec)

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
	log.Printf("file bytes: %v\n", buff.Bytes())
	return buff, nil
}


func processClusterData(buff *bytes.Buffer, db *bolt.DB)([]RecordBatch, error){
	//first we need to extract the actual length of the bytes
	//from this data we can then use to generate batch records
	batches := make([]RecordBatch, 0)

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

		batchBuff := bytes.NewBuffer(batchBytes)
		
		err := batchReader(&batch,batchBuff, db)
		if err != nil{
			log.Printf("error while batch reading: %v\n", err)
			return nil, errors.New("error while batch reading")
		}

		batches = append(batches, batch)
	}

	return batches, nil
}


func batchReader(recordBatch *RecordBatch, buff *bytes.Buffer, db *bolt.DB)(error){

	var partitionLeaderEpoch int32
	if err := binary.Read(buff, binary.BigEndian, &partitionLeaderEpoch); err != nil {
		log.Printf("error while reading partitionLeaderEpoch: %v\n", err)
		return errors.New("error while reading partition leader epoch")
	}
	recordBatch.PartitionLeaderEpoch = partitionLeaderEpoch

	magicByte, err:= buff.ReadByte()
	if err != nil {
		log.Printf("error while reading magic byte: %v\n", err)
		return errors.New("error while reading magic byte")
	}
	recordBatch.MagicByte = magicByte

	var crc int32
	if err := binary.Read(buff, binary.BigEndian, &crc); err != nil {
		log.Printf("error while reading crc: %v\n", err)
		return errors.New("error while reading crc")
	}
	recordBatch.CRC = crc

	var attr int16
	if err := binary.Read(buff, binary.BigEndian, &attr); err != nil {
		log.Printf("error while reading attr: %v\n", err)
		return  errors.New("error while reading attr")
	}
	recordBatch.Attributes = attr

	var lastOffsetDelta int32
	if err := binary.Read(buff, binary.BigEndian, &lastOffsetDelta); err != nil {
		log.Printf("error while reading last offset delta: %v\n", err)
		return  errors.New("error while reading last offset delta")
	}
	recordBatch.LastOffsetDelta = lastOffsetDelta

	var baseOffsetTimeStamp int64
	if err := binary.Read(buff, binary.BigEndian, &baseOffsetTimeStamp); err != nil {
		log.Printf("error while reading base offset timestamp: %v\n", err)
		return errors.New("error while reading base offset timestamp")
	}
	recordBatch.BaseTimeStamp = baseOffsetTimeStamp

	var maxTimeStamp int64
	if err := binary.Read(buff, binary.BigEndian, &maxTimeStamp); err != nil {
		log.Printf("error while reading max offset timestamp: %v\n", err)
		return errors.New("error while extracting offset timestamp")
	}
	recordBatch.MaxTimeStamp = maxTimeStamp

	var producerId int64
	if err := binary.Read(buff, binary.BigEndian, &producerId); err != nil {
		log.Printf("error while reading max offset producerId: %v\n", err)
		return  errors.New("error while extracting offset producerId")
	}
	recordBatch.ProducerId = producerId

	var producerEpoch int16
	if err := binary.Read(buff, binary.BigEndian, &producerEpoch); err != nil {
		log.Printf("error while reading max offset producerId: %v\n", err)
		return  errors.New("error while extracting offset producerId")
	}
	recordBatch.ProducerEpoch = producerEpoch

	var baseSequence int32
	if err := binary.Read(buff, binary.BigEndian, &baseSequence); err != nil {
		log.Printf("error while reading base sequence: %v\n", err)
		return  errors.New("error while extracting base sequence")
	}
	recordBatch.BaseSequence = baseSequence

	var recordLen int32
	if err := binary.Read(buff, binary.BigEndian, &recordLen); err != nil {
		log.Printf("error while reading record len: %v\n", err)
		return  errors.New("error while extracting record len")
	}
	recordBatch.RecordsLen = uint32(recordLen) //used to indicate the no of expected records

	records := make([]Record, recordLen)

	for i := 0; i < int(recordLen); i++ {
		record, err := newRecordReader(buff, db)
		if err != nil {
			log.Printf("error while creating new record: %v\n", err)
			return  errors.New("error while creating new record")
		}
		records = append(records, *record)
	}

	recordBatch.Records = records

	return nil
}

func newRecordReader(buff *bytes.Buffer, db *bolt.DB)(*Record, error){
	record := Record{}
	
	contentLen, err := ReadZigZag(buff) 
	if err != nil {
		log.Printf("error while reading record content len: %v\n", err)
		return nil, errors.New("error while extracting content len")
	}
	record.Length = contentLen

	attr, err := buff.ReadByte()
	if err != nil {
		log.Printf("error while reading record attr: %v\n", err)
		return nil, errors.New("error while extracting record attr")
	}
	record.Attributes = int8(attr)

	timeStampDelta, err := ReadZigZag(buff)
	if err != nil {
		log.Printf("error whle reading timestamp delta: %v\n", err)
		return nil, errors.New("error while extracting timestamp delta")
	}
	record.TimeStampDelta = timeStampDelta

	offsetDelta, err := ReadZigZag(buff)
	if err != nil {
		log.Printf("error while reading offset delta: %v\n", err)
		return nil, errors.New("error while extracting offset delta")
	}
	record.OffsetDelta = offsetDelta

	keyLen, err := ReadZigZag(buff)
	if err != nil {
		log.Printf("error while reading keyLen %v\n", err)
		return nil, errors.New("error while extracting keyLen")
	}
	record.KeyLen = keyLen

	log.Printf("record key len: %v\n", record.KeyLen)

	if keyLen > 0 {
		key := make([]byte, keyLen)
		if _, err := buff.Read(key); err != nil {
			log.Printf("error while reading key variable: %v\n", err)
			return nil, errors.New("error while reading key variable")
		}
		record.Key = key
	}

	valLen, err := ReadZigZag(buff)
	if err != nil {
		log.Printf("error while reading valLen %v\n", err)
		return nil, errors.New("error while extracting valLen")
	}
	record.ValLen = valLen

	if valLen > 0 {
		log.Printf("record val len: %v\n", valLen)
		//here we are gonna make a copy of the val cont for manipulation
		//then after that we are gonna store the original content
		val := make([]byte, valLen)
		if _, err := buff.Read(val); err != nil {
			log.Printf("error while reading val: %v\n", err)
			return nil, errors.New("error while extracting val")
		}
		record.Val = val

		//create a new buff for val content manipulation
		valBuff := bytes.NewBuffer(val)
		valHeader, err := newValHeader(valBuff, db)
		if err != nil {
			log.Printf("error while creating val header: %v\n", err)
			return nil, errors.New("error while creating val header")
		}
		err = valHeader.processType(valBuff)
		if err != nil {
			log.Printf("error while processing record type: %v\n", err)
			return nil, errors.New("error while processing record type")
		}
	}

	headersArrCount, err := buff.ReadByte()
	if err != nil {
		log.Printf("error while fetching headers arr count: %v\n", err)
		return nil, err
	}
	record.HeadersArrCount = uint8(headersArrCount)
	return &record, nil
}

func newValHeader(valBuff *bytes.Buffer, db *bolt.DB)(*ValHeader, error){
	header := ValHeader{}
	frameVersion, err := valBuff.ReadByte()
	if err != nil {
		log.Printf("error while recording new feature level: %v\n", err)
		return nil, errors.New("error while extracting feature level")
	}
	header.FrameVersion = int8(frameVersion)

	recType, err := valBuff.ReadByte()
	if err != nil {
		log.Printf("error while readig record type: %v\n", err)
		return nil, errors.New("error while extracting record type")
	}
	header.RecordType = int8(recType)

	version, err := valBuff.ReadByte()
	if err != nil {
		log.Printf("error while reading version: %v\n", err)
		return nil, errors.New("error while extracting record version")
	}
	header.Version = int8(version)

	header.db = db

	return &header, nil
}


func (valHeader *ValHeader) processType(valBuff *bytes.Buffer)(error){
	log.Printf("val header record type: %v\n", valHeader.RecordType)

	switch valHeader.RecordType {
	case 12:
		feature := FeatureLevelRec{}
		feature.Header = *valHeader

		nameLen, err := ReadVariant(valBuff)
		if err != nil {
			log.Printf("error while reading name length: %v\n", err)
			return errors.New("error while reading name length")
		}
		feature.NameLen = nameLen	

		name := make([]byte, nameLen)
		if _, err := valBuff.Read(name); err != nil {
			log.Printf("error while reading name content: %v\n", err)
			return errors.New("error while reading name content")
		}
		feature.Name = name	

		var featureLevel int16
		if err := binary.Read(valBuff, binary.BigEndian, &featureLevel); err != nil {
			log.Printf("error while reading feature level: %v\n", err)
			return errors.New("error while reading feature level")
		}
		feature.Level = featureLevel	

		tag, err := ReadZigZag(valBuff)
		if err != nil {
			log.Printf("error while reading tag: %v\n", err)
			return errors.New("error while extracting tag")
		}
		feature.Tag = tag

		err = CreateFeatureBucket(valHeader.db)
		if err != nil {
			log.Printf("database error: %v\n", err)
			return err
		}

		err = PutItemsFeatureBucket(valHeader.db, string(feature.Name), feature)
		if err != nil {
			log.Printf("database error: %v\n", err)
			return err
		}
		break

	case 2:
		topic := TopicLevelRec{}
		topic.Header = *valHeader

		topicNameLen, err := ReadVariant(valBuff)
		if err != nil {
			log.Printf("error while reading topic name: %v\n", err)
			return errors.New("error while reading topic name")
		}
		topic.NameLen = topicNameLen	

		topicContent := make([]byte, topicNameLen)
		if _, err := valBuff.Read(topicContent); err != nil {
			log.Printf("error while reading topic content: %v\n", err)
			return err
		}
		topic.Name = topicContent	
		lR := io.LimitReader(valBuff, 16)
		var topicId []byte
		if _, err := lR.Read(topicId); err != nil {
			log.Printf("error while limit reading the topic uuid: %v\n", err)
			return err
		}

		//parse the uuid and check whether it is a valid uuid
		validId, err := uuid.ParseBytes(topicId)
		if err != nil {
			log.Printf("error while parsing topic id: %v\n", err)
			return errors.New("error while parsing topic id")
		}
		topic.Id = validId

		tag, err := ReadZigZag(valBuff)
		if err != nil {
			log.Printf("error while reading byte: %v\n", err)
			return err
		}
		topic.Tag = tag	

		err = CreateTopicBucket(valHeader.db)
		if err != nil {
			log.Printf("database error: %v\n", err)
			return err
		}

		err = PutItemsTopicBucket(valHeader.db, string(topic.Name), topic)
		if err != nil {
			log.Printf("database error: %v\n", err)
			return err
		}

		break
	case 3:
		partitionsRec := PartitionRec{} 

		partitionsRec.Header = *valHeader
		var partitionsId int32
		if err := binary.Read(valBuff, binary.BigEndian, &partitionsId); err != nil {
			log.Printf("error while reading partitions id")
			return err
		}

		lR := io.LimitReader(valBuff, 16)
		var topicId []byte
		if _, err := lR.Read(topicId); err != nil {
			log.Printf("error while limit reading the topic uuid: %v\n", err)
			return err
		}
		//parse the uuid and check whether it is a valid uuid
		validId, err := uuid.ParseBytes(topicId)
		if err != nil {
			log.Printf("error while parsing topic id: %v\n", err)
			return errors.New("error while parsing topic id")
		}
		partitionsRec.TopicId = validId

		replicArrlen, err := ReadVariant(valBuff) 
		if err != nil {
			log.Printf("error while creating replic arr len %v\n", err)
			return errors.New("error while extracting replic arr len")
		}
	  partitionsRec.ReplicArrLen = replicArrlen
		replicArr := make([]int32, replicArrlen - 1)
		for i := 0; i < int(replicArrlen - 1); i++ {
			var replic int32
			if err := binary.Read(valBuff, binary.BigEndian, &replic); err != nil {
				log.Printf("error while extracting to replic buff: %v\n", err)
				return errors.New("error while extracting replic buff")
			}
			replicArr[i] = replic
		}
		partitionsRec.ReplicArr = replicArr

		inSyncArrLen, err := ReadVariant(valBuff)
		if err != nil {
			log.Printf("error while creating in sync replic arr len %v\n", err)
			return errors.New("error while extracting  in sync replic arr len")
		}
	  partitionsRec.SyncReplicArrLen = inSyncArrLen
		inSyncArr := make([]int32, inSyncArrLen - 1)
		for i := 0; i < int(replicArrlen - 1); i++ {
			var sync int32
			if err := binary.Read(valBuff, binary.BigEndian, &sync); err != nil {
				log.Printf("error while extracting to in sync replic buff: %v\n", err)
				return errors.New("error while extracting in sync replic buff")
			}
			inSyncArr[i] = sync
		}
	  partitionsRec.SyncReplicaArr = inSyncArr
	
		remReplicArr, err := ReadVariant(valBuff)
		if err != nil {
			log.Printf("error while reading remReplicArr: %v\n", err)
			return err
		}
	  partitionsRec.RemoveReplicaLenArr = remReplicArr - 1

		addReplicArr, err := ReadVariant(valBuff) 
		if err != nil {
			log.Printf("error while reading addReplicArr: %v\n", err)
			return err
		}
		partitionsRec.AddReplicaArr = addReplicArr

		var leaderId int32
	  if err := binary.Read(valBuff, binary.BigEndian, &leaderId); err != nil {
			log.Printf("error while reading leaderId: %v\n", err)
			return err
		}
	  partitionsRec.Leader = leaderId

		var leaderEpoch int32
		if err := binary.Read(valBuff, binary.BigEndian, &leaderEpoch); err != nil {
			log.Printf("error while reading leaderEpoch: %v\n", err)
			return err
		}
		partitionsRec.LeaderEpoch = leaderEpoch

		var partitionEpoch int32
		if err := binary.Read(valBuff, binary.BigEndian, &partitionEpoch); err != nil {
			log.Printf("error while reading leaderEpoch: %v\n", err)
			return err
		}
		partitionsRec.PartitionEpoch = partitionEpoch

		directoriesArrLen, err := ReadVariant(valBuff)
		if err != nil {
			log.Printf("error while creating in sync replic arr len %v\n", err)
			return errors.New("error while extracting  in sync replic arr len")
		}
		partitionsRec.DirectoriesLen = directoriesArrLen


		directoriesArr := make([][]byte,  directoriesArrLen- 1)
		for i := 0; i < int(directoriesArrLen - 1); i++ {
			var dir []byte
			lR := io.LimitReader(valBuff, 16)
			if _,err := lR.Read(dir); err != nil {
				log.Printf("error while reading dir content: %v\n", err)
				return err
			}
			directoriesArr[i]= dir
		}
		partitionsRec.DirectoryArr = directoriesArr

		tag, err := ReadZigZag(valBuff)		
		if err != nil {
			log.Printf("error while reading tag: %v\n", err)
			return err
		}
		partitionsRec.Tag = tag 

		err = CreatePartitionBucket(valHeader.db)
		if err != nil {
			log.Printf("database error: %v\n", err)
			return err
		}

		err = PutItemsPartitionBucket(valHeader.db, string(validId[:]), partitionsRec)
		if err != nil {
			log.Printf("database error: %v\n", err)
			return err
		}

		break
	}	
	
	return  nil
}

 func ReadClusterFile(db *bolt.DB) (error) {
	log.Println("reading cluster file")
	buff, err := readClusterMetaData(PATH)
	if err != nil {
		log.Printf("error while reading cluster meta data: %v\n", err)
		return err 	
	}


	_, err = processClusterData(buff, db)
	if err != nil {
		log.Printf("error while processing cluster meta data: %v\n", err)
		return err 	
	}


	log.Println("finished reading cluster file")

	return nil
}
