package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	ProcessedMessages = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "Processed_messages",
			Help: "Number of processed messages",
		},
	)

	MessageProcessingTime = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name: "Processing_messages_time_seconds",
			Help: "Time taken to process messages",
		},
	)

	DLQMessages = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "Transferred_messages_to_DLQ",
			Help: "Number of messages transferred to DLQ",
		},
	)
)

func Register() {
	prometheus.MustRegister(ProcessedMessages, MessageProcessingTime)
}
