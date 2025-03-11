package repository

import (
	"context"
	"errors"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2"
	"log/slog"
	"payments-worker/internal/domain"
)

type PurchaseRepository struct {
	db  clickhouse.Conn
	log *slog.Logger
}

func NewPurchaseRepository(db clickhouse.Conn, log *slog.Logger) *PurchaseRepository {
	return &PurchaseRepository{
		db:  db,
		log: log,
	}
}

func (r *PurchaseRepository) SavePurchasesAndReturnFailedIDs(ctx context.Context, purchases []domain.Purchase) ([]string, error) {
	if len(purchases) == 0 {
		return nil, nil
	}

	batch, err := r.db.PrepareBatch(ctx, `
		INSERT INTO purchases (
			purchase_id, user_id, trainer_id, timestamp,
			training_type, training_id, training_title, training_category,
			price_cents, discount_cents, session_date,
			payment_method, payment_status, payment_amount_cents, payment_currency
		) VALUES`)
	if err != nil {
		r.log.Error("failed to prepare batch", slog.Any("error", err))
		return extractIDs(purchases), err
	}

	var failedIDs []string
	var allErrors []error

	for _, p := range purchases {
		if err := batch.Append(
			p.PurchaseID, p.UserID, p.TrainerID, p.Timestamp,
			p.Training.Type, p.Training.TrainingID, p.Training.Title, p.Training.Category,
			p.Training.PriceCents, p.Training.DiscountCents, p.Training.SessionDate,
			p.Payment.Method, p.Payment.Status, p.Payment.AmountCents, p.Payment.Currency,
		); err != nil {
			r.log.Error("failed to append record", slog.Any("error", err))
			failedIDs = append(failedIDs, p.PurchaseID)
			allErrors = append(allErrors, err)
		}
	}

	// Отправляем только если есть успешные записи
	if len(failedIDs) < len(purchases) {
		if err := batch.Send(); err != nil {
			r.log.Error("failed to send batch", slog.Any("error", err))
			allErrors = append(allErrors, err)
			// Добавляем в failedIDs все записи, которые успешно добавились в batch, но не были отправлены
			failedIDs = extractIDs(purchases[len(failedIDs):])
		}
	}

	// Логируем успешные и неудачные операции
	defer func() {
		if len(failedIDs) == 0 {
			r.log.Info("batch of purchases saved", slog.Int("count", len(purchases)))
		} else {
			r.log.Warn("some records failed", slog.Int("count", len(failedIDs)))
		}
	}()

	// Если все записи сохранены — ошибки нет
	if len(failedIDs) == 0 {
		return nil, nil
	}

	// Создаем агрегированную ошибку
	finalErr := fmt.Errorf("failed to save %d of %d records: %w", len(failedIDs), len(purchases), errors.Join(allErrors...))

	return failedIDs, finalErr
}

func (r *PurchaseRepository) Close() error {
	return r.db.Close()
}

func extractIDs(purchases []domain.Purchase) []string {
	ids := make([]string, 0, len(purchases))
	for _, p := range purchases {
		ids = append(ids, p.PurchaseID)
	}
	return ids
}
