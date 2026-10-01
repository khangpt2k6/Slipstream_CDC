// Package metrics holds every Prometheus instrument Slipstream exports and the
// handler that serves them. Instruments live in one place so dashboards and
// alerts have a single source of truth for names and labels.
//
// Pipeline instruments carry a "pipeline" label (docs, identity, ...) because
// one indexer process runs several consume loops side by side.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Consume loop (internal/pipeline).
var (
	// EventsConsumed counts records polled from Kafka, including ones later
	// skipped or dead-lettered. rate() over it is consume throughput.
	EventsConsumed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ss_events_consumed_total",
		Help: "Kafka records polled by a pipeline.",
	}, []string{"pipeline"})

	// ItemsWritten counts items handed to a sink in successful flushes.
	ItemsWritten = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ss_items_written_total",
		Help: "Items written by successful sink flushes.",
	}, []string{"pipeline"})

	// BatchesFlushed counts successful, non-empty flushes.
	BatchesFlushed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ss_batches_flushed_total",
		Help: "Successful sink flushes.",
	}, []string{"pipeline"})

	// FlushDuration observes each flush attempt, including failed ones.
	FlushDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ss_flush_duration_seconds",
		Help:    "Duration of a sink flush attempt.",
		Buckets: []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5},
	}, []string{"pipeline"})

	// BufferedItems is the current buffer depth. It stays at or below the
	// batch size; a stuck non-zero value means the sink is stalled.
	BufferedItems = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ss_buffered_items",
		Help: "Items buffered awaiting a flush.",
	}, []string{"pipeline"})

	// SinkRetries counts failed flushes that were retried with backoff.
	SinkRetries = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ss_sink_retries_total",
		Help: "Sink flush attempts retried after a failure.",
	}, []string{"pipeline"})

	// DLQTotal counts records routed to a dead-letter topic.
	DLQTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ss_dlq_total",
		Help: "Records routed to the dead-letter topic (undecodable).",
	}, []string{"pipeline"})

	// ConsumerLag is uncommitted records per group, topic and partition.
	ConsumerLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "ss_consumer_lag",
		Help: "Consumer group lag (uncommitted records) per topic and partition.",
	}, []string{"group", "topic", "partition"})
)

// Document index (internal/index).
var (
	// DocsApplied counts document writes that changed the index, by op.
	DocsApplied = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ss_docs_applied_total",
		Help: "Document events applied to the search index.",
	}, []string{"op"})

	// DocsDiscarded counts events dropped because the index already held the
	// same or a newer version: duplicates and late, out-of-order deliveries.
	DocsDiscarded = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ss_docs_discarded_total",
		Help: "Document events discarded by version check (duplicate or stale).",
	}, []string{"reason"})

	// CASConflicts counts optimistic-concurrency conflicts that forced a
	// re-plan of part of a batch (a concurrent writer touched the doc).
	CASConflicts = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ss_cas_conflicts_total",
		Help: "Index writes rejected by if_seq_no and re-planned.",
	})

	// Quarantined counts docs hidden because an update for them was poison.
	Quarantined = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ss_docs_quarantined_total",
		Help: "Documents hidden because an event for them could not be decoded.",
	})

	// IndexFreshness is source change time to index ack, for live events.
	IndexFreshness = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ss_index_freshness_seconds",
		Help:    "Source change to search-index ack, live events only.",
		Buckets: []float64{.025, .05, .1, .2, .3, .5, .75, 1, 1.5, 2, 3, 5, 10, 30},
	}, []string{"datasource"})
)

// Identity graph (internal/acl).
var (
	// EdgesApplied counts identity edge changes applied to the graph.
	EdgesApplied = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ss_identity_edges_applied_total",
		Help: "Identity edge changes applied to the permission graph.",
	}, []string{"result"})

	// ACLPropagation is directory commit to graph apply, per edge change.
	ACLPropagation = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "ss_acl_propagation_seconds",
		Help:    "Identity change commit to permission graph apply.",
		Buckets: []float64{.005, .01, .025, .05, .1, .2, .3, .5, .75, 1, 2, 5},
	})
)

// Connectors and serving.
var (
	// ConnectorEvents counts events emitted by connectors.
	ConnectorEvents = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ss_connector_events_total",
		Help: "Events emitted by a connector.",
	}, []string{"source", "mode"})

	// DeliveriesDeduped counts webhook deliveries dropped as already seen.
	DeliveriesDeduped = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "ss_deliveries_deduped_total",
		Help: "Webhook or event deliveries dropped as duplicates.",
	}, []string{"source"})

	// SearchLatency is end-to-end search handler time by retrieval mode.
	SearchLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "ss_search_latency_seconds",
		Help:    "Search request latency by retrieval mode.",
		Buckets: []float64{.005, .01, .02, .03, .05, .075, .1, .15, .2, .3, .5, 1},
	}, []string{"mode"})

	// EmbedBacklog is docs whose embedding is missing or stale.
	EmbedBacklog = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "ss_embed_backlog",
		Help: "Documents waiting for an up-to-date embedding.",
	})

	// Embedded counts embeddings written.
	Embedded = promauto.NewCounter(prometheus.CounterOpts{
		Name: "ss_embedded_total",
		Help: "Document embeddings written to the index.",
	})
)

// Handler serves the registered metrics in Prometheus text format.
func Handler() http.Handler { return promhttp.Handler() }
