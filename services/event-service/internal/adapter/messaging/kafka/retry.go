package kafka

import "time"

const defaultMaxAttempts = 5

type RetryPolicy struct {
	MaxAttempts  int
	FirstBackoff time.Duration
	MaxBackoff   time.Duration
}

func DefaultRetryPolicy(maxAttempts int) RetryPolicy {
	if maxAttempts < 1 {
		maxAttempts = defaultMaxAttempts
	}
	return RetryPolicy{
		MaxAttempts:  maxAttempts,
		FirstBackoff: 250 * time.Millisecond,
		MaxBackoff:   30 * time.Second,
	}
}

func (p RetryPolicy) Backoff(attempt int) time.Duration {
	doublings := attempt - 1
	if doublings < 0 {
		doublings = 0
	}
	if doublings > 20 {
		doublings = 20
	}
	wait := p.FirstBackoff << doublings
	if wait > p.MaxBackoff {
		return p.MaxBackoff
	}
	return wait
}
