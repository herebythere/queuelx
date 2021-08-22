package queuelx

import (
	"context"
	"fmt"
	"testing"
	"time"
	// "os"

	sclx "github.com/herebythere/supercachelx/v0.1/golang"
)

const (
	testQueue = "test_queue"
)

var (
	// localCacheAddress = os.Getenv("LOCAL_CACHE_ADDRESS")
	localCacheAddress = "http://10.88.0.1:6050"

	testIdentifier   = "test_queue_local"
	testQueuePayload = QueuePayload{
		Address:  "https://example.com",
		Method:   "GET",
		Timestep: time.Now().Unix(),
	}
)

func resetHeadAndTailSentinels() {
	headID := getCacheSetID(testIdentifier, headSentinel)
	headInstructions := []interface{}{setCache, headID, 0, expCache, dayInSeconds}
	sclx.ExecInstructionsAndParseString(
		localCacheAddress,
		&headInstructions,
	)

	tailID := getCacheSetID(testIdentifier, tailSentinel)
	tailInstructions := []interface{}{setCache, tailID, 0, expCache, dayInSeconds}
	sclx.ExecInstructionsAndParseString(
		localCacheAddress,
		&tailInstructions,
	)
}

func TestQueueCallbackDoesNotIncrement(t *testing.T) {
	resetHeadAndTailSentinels()

	callbackCount := 0

	var queueCallback QueueCallback
	queueCallback = func(
		payload *QueuePayload,
		cancelCallback *context.CancelFunc,
		err error,
	) error {
		callbackCount += 1

		return nil
	}

	queue := Queue{
		cacheAddress:   localCacheAddress,
		identifier:     testIdentifier,
		delay:          2 * int64(time.Second),
		callback:       &queueCallback,
		cancelCallback: nil,
	}

	go queue.Run()
	time.Sleep(1 * time.Second)
	queue.Cancel()

	if callbackCount != 0 {
		t.Fail()
		t.Logf(fmt.Sprint("callback count was: ", callbackCount))
	}
}

func TestQueueCallbackIncrements(t *testing.T) {
	resetHeadAndTailSentinels()

	callbackCount := 0

	var queueCallback QueueCallback
	queueCallback = func(
		payload *QueuePayload,
		cancelCallback *context.CancelFunc,
		err error,
	) error {
		callbackCount += 1

		return nil
	}

	queue := Queue{
		cacheAddress:   localCacheAddress,
		identifier:     testIdentifier,
		delay:          2 * int64(time.Second),
		callback:       &queueCallback,
		cancelCallback: nil,
	}

	// add queues
	index := 5
	for index > 0 {
		queue.Enqueue(&testQueuePayload)
		index -= 1
	}

	go queue.Run()
	time.Sleep(2 * time.Second)
	queue.Cancel()

	if callbackCount != 5 {
		t.Fail()
		t.Logf(fmt.Sprint("callback count was: ", callbackCount))
	}
}

func TestQueueCallbackIncrementsWithDelay(t *testing.T) {
	resetHeadAndTailSentinels()

	callbackCount := 0

	var queueCallback QueueCallback
	queueCallback = func(
		payload *QueuePayload,
		cancelCallback *context.CancelFunc,
		err error,
	) error {
		callbackCount += 1

		return nil
	}

	queue := Queue{
		cacheAddress:   localCacheAddress,
		identifier:     testIdentifier,
		delay:          5 * int64(time.Second),
		callback:       &queueCallback,
		cancelCallback: nil,
	}

	// add queues
	preIndex := 5
	for preIndex > 0 {
		queue.Enqueue(&testQueuePayload)
		preIndex -= 1
	}

	go queue.Run()
	time.Sleep(2 * time.Second)

	postIndex := 5
	for postIndex > 0 {
		queue.Enqueue(&testQueuePayload)
		postIndex -= 1
	}

	time.Sleep(2 * time.Second)
	queue.Cancel()

	if callbackCount != 5 {
		t.Fail()
		t.Logf(fmt.Sprint("callback count was: ", callbackCount))
	}
}
