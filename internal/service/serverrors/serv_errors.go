package serverrors

import "errors"

var (
	ErrUnmarshalMessage = errors.New("failed to unmarshal Kafka message")
	ErrInvalidPurchase  = errors.New("invalid purchase model")
)
