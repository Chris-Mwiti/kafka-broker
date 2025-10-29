package main

type BatchRecord struct {
	BaseOffset int64 //offset batch of the record
	BatchLength 	uint32 //lenght of the total items in the batch

	PartitionLeaderEpoch uint32 //used to identify the epoch of the leader in this partition
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
	Val []RecordVal
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
	DirectoryArr []byte
	Tag uint8
} 

