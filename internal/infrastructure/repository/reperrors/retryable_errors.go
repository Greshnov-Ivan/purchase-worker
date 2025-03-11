package reperrors

import (
	"errors"
	"github.com/ClickHouse/clickhouse-go/v2"
)

func IsRetryableError(err error) bool {
	var exception *clickhouse.Exception

	// Обходим все вложенные ошибки
	for {
		if errors.As(err, &exception) {
			switch exception.Code {
			case 209, 516, 160, 241, 319, 1002: // Коды ошибок, которые можно ретраить
				return true
			}
		}

		// Разворачиваем err, если это errors.Join или fmt.Errorf("wrap: %w", err)
		nextErr := errors.Unwrap(err)
		if nextErr == nil {
			break // Если больше нет вложенных ошибок — выходим
		}
		err = nextErr
	}

	return false
}
