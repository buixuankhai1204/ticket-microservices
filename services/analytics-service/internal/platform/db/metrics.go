package db

import (
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

type poolCollector struct {
	pool              *pgxpool.Pool
	inUse             *prometheus.Desc
	idle              *prometheus.Desc
	max               *prometheus.Desc
	acquires          *prometheus.Desc
	emptyAcquires     *prometheus.Desc
	emptyAcquireWaitS *prometheus.Desc
	canceledAcquires  *prometheus.Desc
}

func RegisterPoolMetrics(pool *pgxpool.Pool) {
	prometheus.MustRegister(newPoolCollector(pool))
}

func newPoolCollector(pool *pgxpool.Pool) *poolCollector {
	return &poolCollector{
		pool:              pool,
		inUse:             prometheus.NewDesc("db_pool_connections_in_use", "Connections currently checked out of the pool.", nil, nil),
		idle:              prometheus.NewDesc("db_pool_connections_idle", "Open connections sitting idle in the pool.", nil, nil),
		max:               prometheus.NewDesc("db_pool_connections_max", "Configured maximum pool size.", nil, nil),
		acquires:          prometheus.NewDesc("db_pool_acquires_total", "Total connection acquires.", nil, nil),
		emptyAcquires:     prometheus.NewDesc("db_pool_empty_acquires_total", "Acquires that had to wait because the pool had no idle connection.", nil, nil),
		emptyAcquireWaitS: prometheus.NewDesc("db_pool_empty_acquire_wait_seconds_total", "Cumulative seconds spent waiting on acquires that found the pool empty.", nil, nil),
		canceledAcquires:  prometheus.NewDesc("db_pool_canceled_acquires_total", "Acquires abandoned because the caller's context ended while waiting.", nil, nil),
	}
}

func (c *poolCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.inUse
	ch <- c.idle
	ch <- c.max
	ch <- c.acquires
	ch <- c.emptyAcquires
	ch <- c.emptyAcquireWaitS
	ch <- c.canceledAcquires
}

func (c *poolCollector) Collect(ch chan<- prometheus.Metric) {
	s := c.pool.Stat()
	ch <- prometheus.MustNewConstMetric(c.inUse, prometheus.GaugeValue, float64(s.AcquiredConns()))
	ch <- prometheus.MustNewConstMetric(c.idle, prometheus.GaugeValue, float64(s.IdleConns()))
	ch <- prometheus.MustNewConstMetric(c.max, prometheus.GaugeValue, float64(s.MaxConns()))
	ch <- prometheus.MustNewConstMetric(c.acquires, prometheus.CounterValue, float64(s.AcquireCount()))
	ch <- prometheus.MustNewConstMetric(c.emptyAcquires, prometheus.CounterValue, float64(s.EmptyAcquireCount()))
	ch <- prometheus.MustNewConstMetric(c.emptyAcquireWaitS, prometheus.CounterValue, s.EmptyAcquireWaitTime().Seconds())
	ch <- prometheus.MustNewConstMetric(c.canceledAcquires, prometheus.CounterValue, float64(s.CanceledAcquireCount()))
}
