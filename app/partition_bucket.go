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
		existingBytes := bucket.Get([]byte(key))

		var items []PartitionRec

		if existingBytes != nil {
			//decode the existing bytes
			decoder := gob.NewDecoder(bytes.NewReader(existingBytes))
			if err := decoder.Decode(&items); err != nil {
				return err
			}
		}

		//append the new item to the decoded items
		items = append(items, item)
		var buff bytes.Buffer
		encoder := gob.NewEncoder(&buff)
		if err := encoder.Encode(items); err != nil {
			return err
		}

		if err := bucket.Put([]byte(key), buff.Bytes()); err != nil {
			return err 
		}
		return nil
	})

	if err != nil {
		log.Printf("error: %v\n", err)
		return err
	}
 return nil
}

func GetItemsPartitionBucket(db *bolt.DB, key string)(*[]PartitionRec,error){
	var records []PartitionRec
	err := db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket([]byte("partition_bucket"))
		if bucket == nil {
			return errors.New("bucket does not exist")
		}

		item := bucket.Get([]byte(key))
		decoder := gob.NewDecoder(bytes.NewReader(item))
		err := decoder.Decode(&records)
		if err != nil {
			return err 
		}
		return nil

	})

	if err != nil {
		log.Printf("error while getting item: %v\n", err)
		return nil, err
	}
	return &records, nil
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


