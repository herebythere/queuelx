package queuelx

import (
	"testing"
)

func TestIncrementSentinel(t *testing.T) {
	resetHeadAndTailSentinels()

	count, errCount := incrementSentinel(
		localCacheAddress,
		testIdentifier,
	)
	if errCount != nil {
		t.Fail()
		t.Logf(errCount.Error())
	}
	if count == nil {
		t.Fail()
		t.Logf("increment was not successfuul")
	}
	if count != nil && *count < 1 {
		t.Fail()
		t.Logf("increment was less than 1, which means key might be occupied by non integer")
	}
}

func TestGetSentinels(t *testing.T) {
	sentinels, errSentinels := getSentinels(
		localCacheAddress,
		testIdentifier,
	)
	if errSentinels != nil {
		t.Fail()
		t.Logf(errSentinels.Error())
	}
	if sentinels == nil {
		t.Fail()
		t.Logf("sentinels was not successfuul")
	}
}

func TestSetQueuePayload(t *testing.T) {
	errQueuePayload := setQueuePayload(
		localCacheAddress,
		testIdentifier,
		&testQueuePayload,
	)
	if errQueuePayload != nil {
		t.Fail()
		t.Logf(errQueuePayload.Error())
	}
}

func TestGetQueuePayload(t *testing.T) {
	queuePayload, errQueuePayload := getQueuePayload(
		localCacheAddress,
		testIdentifier,
		nil,
	)
	if errQueuePayload != nil {
		t.Fail()
		t.Logf(errQueuePayload.Error())
	}
	if queuePayload == nil {
		t.Fail()
		t.Logf("queuePayload should be unsent")
	}
}

func TestAddRequestToQueue(t *testing.T) {
	wasSuccessful, errQueuePayload := addRequestToQueue(
		localCacheAddress,
		testIdentifier,
		&testQueuePayload,
		nil,
	)

	if errQueuePayload != nil {
		t.Fail()
		t.Logf(errQueuePayload.Error())
	}
	if !wasSuccessful {
		t.Fail()
		t.Logf("add request to queue was unsuccessful")
	}
}

func TestUpdateQueueRequests(t *testing.T) {
	queuePayload, errQueuePayload := updateQueueRequests(
		localCacheAddress,
		testIdentifier,
		nil,
	)

	if errQueuePayload != nil {
		t.Fail()
		t.Logf(errQueuePayload.Error())
	}
	if queuePayload == nil {
		t.Fail()
		t.Logf("queue request is nil")
	}
}
