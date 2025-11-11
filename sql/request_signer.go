package sql

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"golang.org/x/exp/slices"
)

func SignRequest(
	accessKeyID string,
	accessKeySecret string,
	method string,
	path string,
	headers map[string]string,
	data []byte,
	queryParams map[string]string,
) string {
	// Calculate body hash BEFORE any transformations
	var bodyHash string

	if len(data) > 0 {
		bodyHashSum := sha256.Sum256(data)
		bodyHash = fmt.Sprintf("%x", bodyHashSum)
	} else {
		emptyBodyHashSum := sha256.Sum256(nil)
		bodyHash = fmt.Sprintf("%x", emptyBodyHashSum)
	}

	// Now transform headers for signing
	for key, value := range headers {
		delete(headers, key)
		headers[TransformHeaderKey(key)] = value
	}

	for key := range headers {
		if !slices.Contains([]string{"content-type", "host", "x-litebase-date"}, key) {
			delete(headers, key)
		}
	}

	// Transform query params
	for key, value := range queryParams {
		delete(queryParams, key)
		queryParams[strings.ToLower(key)] = value
	}

	jsonHeaders, err := json.Marshal(headers)
	var jsonQueryParams []byte

	if len(queryParams) > 0 {
		jsonQueryParams, err = json.Marshal(queryParams)
		if err != nil {
			panic(err)
		}
	} else {
		jsonQueryParams = []byte("{}")
	}

	if err != nil {
		panic(err)
	}

	requestString := strings.Join([]string{
		method,
		fmt.Sprintf("/%s", strings.TrimLeft(path, "/")),
		string(jsonHeaders),
		string(jsonQueryParams),
		bodyHash,
	}, "")

	signedRequestHash := sha256.New()
	signedRequestHash.Write([]byte(requestString))
	signedRequest := fmt.Sprintf("%x", signedRequestHash.Sum(nil))

	dateHash := hmac.New(sha256.New, []byte(accessKeySecret))
	dateHash.Write([]byte(headers["x-litebase-date"]))
	date := fmt.Sprintf("%x", dateHash.Sum(nil))

	serviceHash := hmac.New(sha256.New, []byte(date))
	serviceHash.Write([]byte("litebase_request"))
	service := fmt.Sprintf("%x", serviceHash.Sum(nil))

	signatureHash := hmac.New(sha256.New, []byte(service))
	signatureHash.Write([]byte(signedRequest))
	signature := fmt.Sprintf("%x", signatureHash.Sum(nil))

	token := base64.StdEncoding.EncodeToString(
		fmt.Appendf(nil, "credential=%s;signed_headers=content-type,host,x-litebase-date;signature=%s", accessKeyID, signature),
	)

	return token
}

func TransformHeaderKey(key string) string {
	return strings.ReplaceAll(strings.ToLower(key), "_", "-")
}

// ExtractSignatureFromToken extracts the signature portion from a base64 encoded token
func ExtractSignatureFromToken(token string) (string, error) {
	decoded, err := base64.StdEncoding.DecodeString(token)

	if err != nil {
		return "", fmt.Errorf("failed to decode token: %w", err)
	}

	parts := strings.Split(string(decoded), ";")

	for _, part := range parts {
		if after, ok := strings.CutPrefix(part, "signature="); ok {
			return after, nil
		}
	}

	return "", fmt.Errorf("signature not found in token")
}

// SignChunk creates a signature for a chunk of data using the previous signature.
// This implements the chunked signature scheme similar to AWS Signature Version 4 for LQTP.
//
// Signature calculation:
//  1. Hash the chunk data: chunkHash = SHA256(chunkData)
//  2. Create string to sign: stringToSign = previousSignature + chunkHash
//  3. Generate signing key chain:
//     - dateKey = HMAC-SHA256(accessKeySecret, date)
//     - serviceKey = HMAC-SHA256(dateKey, "litebase_request")
//  4. Sign: signature = HMAC-SHA256(serviceKey, stringToSign)
//
// The signature chains ensure chunks are sent in the correct order and prevents tampering.
func SignChunk(
	accessKeySecret string,
	date string,
	previousSignature string,
	chunkData []byte,
) string {
	// Calculate the hash of the chunk data
	chunkHashSum := sha256.Sum256(chunkData)
	chunkHash := fmt.Sprintf("%x", chunkHashSum)

	// Create the string to sign for this chunk
	// Format: previousSignature + chunkHash
	stringToSign := previousSignature + chunkHash

	// Create the signing key chain (same as in SignRequest)
	dateHash := hmac.New(sha256.New, []byte(accessKeySecret))
	dateHash.Write([]byte(date))
	dateKey := fmt.Sprintf("%x", dateHash.Sum(nil))

	serviceHash := hmac.New(sha256.New, []byte(dateKey))
	serviceHash.Write([]byte("litebase_request"))
	serviceKey := fmt.Sprintf("%x", serviceHash.Sum(nil))

	// Sign the chunk
	signatureHash := hmac.New(sha256.New, []byte(serviceKey))
	signatureHash.Write([]byte(stringToSign))
	signature := fmt.Sprintf("%x", signatureHash.Sum(nil))

	return signature
}
