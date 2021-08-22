package queuelx

import (
	"encoding/json"
	"errors"
	"strconv"
	"strings"

	sclx "github.com/herebythere/supercachelx/v0.1/golang"
)

const (
	colonDelimiter = ":"
	dayInSeconds   = 86400
	expCache       = "EX"
	getCache       = "GET"
	headSentinel   = "head_sentinel"
	incrCache      = "INCR"
	mgetCache      = "MGET"
	okCache        = "OK"
	sentinelValue  = "sentinel_value"
	setCache       = "SET"
	tailSentinel   = "tail_sentinel"
)

var (
	errNilQueuePayload        = errors.New("nil queue payload")
	errRequestFailedToResolve = errors.New("request failed to resolve instructions")
	errSentinelsNotReturned   = errors.New("sentinels were not returned")
)

func getCacheSetID(categories ...string) string {
	return strings.Join(categories, colonDelimiter)
}

func incrementSentinel(
	cacheAddress string,
	identifier string,
) (
	*int64,
	error,
) {
	instructions := []interface{}{incrCache, identifier}

	return sclx.ExecInstructionsAndParseInt64(
		cacheAddress,
		&instructions,
	)
}

func getSentinels(
	cacheAddress string,
	identifier string,
) (
	*[]int64,
	error,
) {
	headID := getCacheSetID(identifier, headSentinel)
	tailID := getCacheSetID(identifier, tailSentinel)
	instructions := []interface{}{mgetCache, headID, tailID}

	sentinels, errSentinels := sclx.ExecInstructionsAndParseMultipleInt64(
		cacheAddress,
		&instructions,
	)
	if errSentinels != nil {
		return nil, errSentinels
	}
	if len(*sentinels) == 2 {
		return sentinels, nil
	}

	return nil, errSentinelsNotReturned
}

func setQueuePayload(
	cacheAddress string,
	identifier string,
	queuePayload *QueuePayload,
) error {
	queuePayloadBytes, errQueuePayloadBytes := json.Marshal(queuePayload)
	if errQueuePayloadBytes != nil {
		return errQueuePayloadBytes
	}

	setID := getCacheSetID(identifier, sentinelValue)
	queuePayloadStr := string(queuePayloadBytes)
	instructions := []interface{}{
		setCache,
		setID,
		queuePayloadStr,
		expCache,
		dayInSeconds,
	}
	execStr, errExecStr := sclx.ExecInstructionsAndParseString(
		cacheAddress,
		&instructions,
	)
	if errExecStr != nil {
		return errExecStr
	}

	if execStr != nil && *execStr == okCache {
		return nil
	}

	// use id to set
	return errRequestFailedToResolve
}

func getQueuePayload(
	cacheAddress string,
	identifier string,
	err error,
) (
	*QueuePayload,
	error,
) {
	if err != nil {
		return nil, err
	}

	setID := getCacheSetID(identifier, sentinelValue)

	instructions := []interface{}{getCache, setID}
	qPayloadBase64, errQPayloadBase64 := sclx.ExecInstructionsAndParseBase64(
		cacheAddress,
		&instructions,
	)
	if errQPayloadBase64 != nil {
		return nil, errQPayloadBase64
	}

	var qPayload QueuePayload
	errUnmarshal := json.Unmarshal(
		[]byte(*qPayloadBase64),
		&qPayload,
	)

	return &qPayload, errUnmarshal
}

func addRequestToQueue(
	cacheAddress string,
	identifier string,
	queuePayload *QueuePayload,
	err error,
) (
	bool,
	error,
) {
	if err != nil {
		return false, err
	}
	if queuePayload == nil {
		return false, errNilQueuePayload
	}

	setID := getCacheSetID(identifier, headSentinel)
	sentinel, errSentinel := incrementSentinel(
		cacheAddress,
		setID,
	)
	if errSentinel != nil {
		return false, errSentinel
	}

	sentinelAsStr := strconv.FormatInt(*sentinel, 10)
	setValueID := getCacheSetID(
		identifier,
		sentinelAsStr,
		sentinelValue,
	)

	errQueuePayload := setQueuePayload(
		cacheAddress,
		setValueID,
		queuePayload,
	)
	if errQueuePayload == nil {
		return true, nil
	}

	return false, errQueuePayload
}

func updateQueueRequests(
	cacheAddress string,
	identifier string,
	err error,
) (
	*QueuePayload,
	error,
) {
	if err != nil {
		return nil, err
	}

	sentinels, errSentinels := getSentinels(
		cacheAddress,
		identifier,
	)
	if errSentinels != nil {
		return nil, errSentinels
	}

	// if tail has caught up to head, skip over
	// [head, tail]
	if (*sentinels)[0] <= (*sentinels)[1] {
		return nil, nil
	}

	// increment tailSentiel
	incrTailID := getCacheSetID(identifier, tailSentinel)
	incrTailSentinel, errIncrTailSentinel := incrementSentinel(
		cacheAddress,
		incrTailID,
	)
	if errIncrTailSentinel != nil {
		return nil, errIncrTailSentinel
	}
	incrTailSentinelStr := strconv.FormatInt(*incrTailSentinel, 10)

	// get queuePayload
	queuePayloadID := getCacheSetID(
		identifier,
		incrTailSentinelStr,
		sentinelValue,
	)

	return getQueuePayload(
		cacheAddress,
		queuePayloadID,
		nil,
	)
}
