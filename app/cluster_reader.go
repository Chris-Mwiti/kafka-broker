package main

type BatchRecord struct {
	BaseOffset [8]byte //offset batch of the record
	BatchLength 	uint32 //lenght of the total items in the batch

	PartitionLeaderEpoch uint32 //used to identify the epoch of the leader in this partition
	MagicByte uint8 //used to identify the version of the record bacth
	CRC int32
	Attributes int16 //used to indicate the attributes of the record batch //bitmask format
	LastOffsetDelta int32 //used to indicate the last offset of this record batch and the base offset
	BaseTimeStamp [8]byte //unix rep of the first batch of record
	MaxTimeStamp [8]byte //used to represent the max timestamp of the records in the batch
	ProducerId [8]byte //used to represent the producer id of the batch records

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
	KeyLen int16 //used to indicate the len of the key 
	Key []byte
	ValLen int16
	Val []RecordVal
	HeadersArrCount uint16
}

type ValHeader struct {
	FrameVersion int8
	Type int8
	TypeVersion int8
}

type RecordVal struct {
	Header ValHeader
	NameLen uint16 //compact array the actual len = len - 1
	Name []byte
	FeatureLev uint16
	TagFieldCount uint16
}
