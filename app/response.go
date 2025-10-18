package main

import (
	"bytes"
	"encoding/binary"
)

type ApiVersionResponse struct {
	header versionResponseHeader
	body versionResponseBody
	messageSize uint32
}

type versionResponseHeader struct {
	correlationId uint32
}

func (r *ApiVersionResponse) Encode() ([]byte, error){
	buff := new(bytes.Buffer)

	if err := binary.Write(buff, binary.BigEndian, r.header.correlationId); err != nil {
		return nil, err
	}

	bodyBytes, err := r.body.Encode()
	if err != nil {
		return nil, err
	}

	//write the body bytes
	if _, err := buff.Write(bodyBytes); err != nil {
		return nil, err	
	}

	r.messageSize = uint32(buff.Len() + 4)
	finalBuf := new(bytes.Buffer)

	if err := binary.Write(finalBuf, binary.BigEndian, r.messageSize); err != nil {
		return nil, err
	}

	if _, err := finalBuf.Write(buff.Bytes()); err != nil {
		return nil, err
	}

	return finalBuf.Bytes(), nil
}

type versionResponseBody struct {
	errorCode uint16
	throttleTime uint32
	tagBuffer uint8
	apiKeys []apiKeyVersion
}

func (b *versionResponseBody) Encode() ([]byte, error){
	buff := new(bytes.Buffer)

	if err := binary.Write(buff,binary.BigEndian, b.errorCode); err != nil {
		return nil,err
	}

	//sets the length of array content length
	if err := buff.WriteByte(byte(len(b.apiKeys) + 1)); err != nil {
		return nil, err
	}

	for _, key := range b.apiKeys {
		keyBytes,err := key.Encode() 
		if err != nil {
			return nil, err
		}

		if _,err := buff.Write(keyBytes); err != nil {
			return nil, err
		}
	}

	if err := binary.Write(buff, binary.BigEndian, b.throttleTime); err != nil {
		return nil, err
	}

	if err := buff.WriteByte(byte(b.tagBuffer)); err != nil {
		return nil, err
	}

	return buff.Bytes(), nil
}

type apiKeyVersion struct {
	ApiKey uint16	
	Minv uint16
	Maxv uint16
	TagBuffer uint8
}

func newApiKeyVersion(apikey, minV, maxV uint16)(apiKeyVersion){
	return apiKeyVersion{
		ApiKey: apikey,
		Minv: minV,
		Maxv: maxV,
		TagBuffer: 0,
	}
}

func (v *apiKeyVersion) Encode()([]byte, error){
	buff := new(bytes.Buffer)

	if err := binary.Write(buff,binary.BigEndian,v.ApiKey); err != nil {
		return nil, err
	}

	if err := binary.Write(buff,binary.BigEndian, v.Minv); err != nil {
		return nil, err
	}

	if err := binary.Write(buff, binary.BigEndian, v.Maxv); err != nil {
		return nil, err
	}
	
	if err := buff.WriteByte(byte(v.TagBuffer)); err != nil {
		return nil,err
	}

	return buff.Bytes(), nil
}

func NewApiVersionResponse(req *ParseRequest) (*ApiVersionResponse){
	if req == nil {
		return nil
	}

	return &ApiVersionResponse{
		header: versionResponseHeader{
			correlationId: req.RequestCorrelationId,
		},
		body: versionResponseBody{
			errorCode: req.ErrorCode(),
			apiKeys: []apiKeyVersion{
				newApiKeyVersion(18,0,4),
			},
		},
	}
}

