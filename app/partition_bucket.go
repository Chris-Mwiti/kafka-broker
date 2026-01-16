package main

import (
	"bytes"
	"encoding/gob"
	"errors"
	"log"

	"github.com/boltdb/bolt"
)

func CreatePartitionBucket(db *bolt.DB)(error){
	err := db.Update(func(tx *bolt.Tx) error {
		_,err := tx.CreateBucketIfNotExists([]byte("partition_bucket"))
		if err != nil {
			return err
		}
		return nil
	})
	
	if err != nil {
		log.Printf("error while creating partition bucket: %v\n", err)
		return err
	}

	return nil
}

func PutItemsPartitionBucket(db *bolt.DB, key string, item PartitionRec)(error){
	err := db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("partition_bucket"))
		if bucket == nil {
			return errors.New("bucket does not exist")
		}
		//encode the item struct to byte format...gob encode
		buff := new(bytes.Buffer)
		encoder := gob.NewEncoder(buff)
		err := encoder.Encode(item)

		if err != nil {
			return errors.Join(errors.New("error while putting item in partition bucket"), err)
		}

		err = bucket.Put([]byte(key), buff.Bytes())
		if err != nil {
			return errors.Join(errors.New("error while putting item in partition bucket"), err)
		}
		return nil
	})

	if err != nil {
		log.Printf("error: %v\n", err)
		return err
	}
	 return nil
}

func GetItemsPartitionBucket(db *bolt.DB, key string)(*PartitionRec,error){
	buff := new(bytes.Buffer)
	var record PartitionRec
	err := db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket([]byte("partition_bucket"))
		if bucket == nil {
			return errors.New("bucket does not exist")
		}

		item := bucket.Get([]byte(key))
		buff.Write(item)
		decoder := gob.NewDecoder(buff)
		err := decoder.Decode(&record)
		if err != nil {
			return err 
		}
		return nil

	})

	if err != nil {
		log.Printf("error while getting item: %v\n", err)
		return nil, err
	}
	return &record, nil
}

func DeleteItemPartitionBucket(db *bolt.DB, key string)(error){
	err := db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("partition_bucket"))

		if bucket == nil {
			return errors.New("bucket does not exist")
		}

		err := bucket.Delete([]byte(key))
		if err != nil {
			return err
		}

		return nil
	})
	
	if err != nil {
		log.Printf("error while getting item: %v\n", err)
		return  err

	}
	return nil
}


