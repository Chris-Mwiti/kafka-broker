package main

import (
	"bytes"
	"encoding/gob"
	"errors"
	"log"

	"github.com/boltdb/bolt"
)

func CreateFeatureBucket(db *bolt.DB)(error){
	err := db.Update(func(tx *bolt.Tx) error {
		_,err := tx.CreateBucketIfNotExists([]byte("feature_bucket"))
		if err != nil {
			return err
		}
		return nil
	})
	
	if err != nil {
		log.Printf("error while creating feature bucket: %v\n", err)
		return err
	}

	return nil
}

func PutItemsFeatureBucket(db *bolt.DB, key string, item FeatureLevelRec)(error){
	err := db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("feature_bucket"))
		if bucket == nil {
			return errors.New("bucket does not exist")
		}
		//encode the item struct to byte format...gob encode
		buff := new(bytes.Buffer)
		encoder := gob.NewEncoder(buff)
		err := encoder.Encode(item)

		if err != nil {
			log.Printf("error while encoding item: %v\n", err)
			return err
		}

		err = bucket.Put([]byte(key), buff.Bytes())
		if err != nil {
			log.Printf("error while putting item in bucket: %v\n", err)
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

func GetItemsFeatureBucket(db *bolt.DB, key string)(*FeatureLevelRec,error){
	buff := new(bytes.Buffer)
	var record FeatureLevelRec
	err := db.View(func(tx *bolt.Tx) error {

		bucket := tx.Bucket([]byte("feature_bucket"))
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



