package queuelx

import (
	"context"
	"errors"
	"time"
)

type QueuePayload struct {
	Address       string            `json:"address"`
	Authorization *string           `json:"authorization,omitempty"`
	Cookies       map[string]string `json:"cookies,omitempty"`
	Method        string            `json:"method"`
	RequestBody   *string           `json:"requestBody,omitempty"`
	Timestep      int64             `json:"timestep"`
}

type QueueCallback func(
	payload *QueuePayload,
	cancelCallback *context.CancelFunc,
	err error,
) error

type Queue struct {
	cacheAddress   string
	identifier     string
	delay          int64
	callback       *QueueCallback
	cancelCallback *context.CancelFunc
}

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
	errRequestFailedToResolve = errors.New("request failed to resolve instructions")
	errInvalidDelayProvided   = errors.New("delay of less than or equal to zero provided")
	errNilQueuePayload        = errors.New("nil queue payload")
	errSentinelsNotReturned   = errors.New("sentinels were not returned")
)

func (q *Queue) Enqueue(queuePayload *QueuePayload) (bool, error) {
	return addRequestToQueue(
		q.cacheAddress,
		q.identifier,
		queuePayload,
		nil,
	)
}

func (q *Queue) Cancel() {
	if q.cancelCallback != nil {
		(*q.cancelCallback)()
	}
}

// Run this function as a goroutine
func (q *Queue) Run() error {
	q.Cancel()
	if q.delay < 1 {
		return errInvalidDelayProvided
	}

	currDelay := int64(-1)
	currNow := time.Now().UnixNano()
	prevNow := currNow
	context, cancel := context.WithCancel(context.Background())

	q.cancelCallback = &cancel

	for {
		select {
		case <-context.Done():
			return context.Err()
		default:
			if currDelay > 0 {
				prevNow = currNow
				currNow = time.Now().UnixNano()
				currDelay -= currNow - prevNow

				continue
			}

			queuePayload, errQueuePayload := updateQueueRequests(
				q.cacheAddress,
				q.identifier,
				nil,
			)
			if queuePayload == nil || errQueuePayload != nil {
				currDelay = q.delay
				continue
			}

			errCallback := (*q.callback)(
				queuePayload,
				&cancel,
				errQueuePayload,
			)
			if errCallback != nil {
				return errCallback
			}
		}
	}

	return nil
}
