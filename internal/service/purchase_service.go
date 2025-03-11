package service

import (
	"context"
	"encoding/json"
	"log/slog"
	"payments-worker/internal/config"
	"payments-worker/internal/metrics"
	"sync"
	"time"

	"github.com/avast/retry-go"
	"payments-worker/internal/domain"
	"payments-worker/internal/infrastructure/repository/reperrors"
)

type PurchaseRepository interface {
	SavePurchasesAndReturnFailedIDs(ctx context.Context, purchases []domain.Purchase) ([]string, error)
}

type DLQProducer interface {
	Send(ctx context.Context, message []byte, err error) error
}

type PurchaseService struct {
	retryConf   config.RetrySaveBatchConfig
	repo        PurchaseRepository
	dlqProducer DLQProducer
	log         *slog.Logger
	batch       []domain.Purchase
	batchSize   int
	mu          sync.Mutex
	flushTicker *time.Ticker
	workerPool  chan struct{}
	wg          sync.WaitGroup
}

func NewPurchaseService(repo PurchaseRepository, dlq DLQProducer, cfg config.ServiceConfig, log *slog.Logger) *PurchaseService {
	s := &PurchaseService{
		retryConf:   cfg.RetrySaveBatchConfig,
		repo:        repo,
		dlqProducer: dlq,
		log:         log,
		batch:       make([]domain.Purchase, 0, cfg.BatchSize),
		batchSize:   cfg.BatchSize,
		flushTicker: time.NewTicker(cfg.FlushInterval),
		workerPool:  make(chan struct{}, cfg.WorkerCount),
	}

	go s.autoFlush()
	return s
}

// ProcessMessage receives raw message, parses and adds to batch.
func (s *PurchaseService) ProcessMessage(ctx context.Context, message []byte) {
	metrics.ProcessedMessages.Inc()
	start := time.Now().UTC()
	defer func() {
		metrics.MessageProcessingTime.Observe(time.Since(start).Seconds())
	}()

	var purchase domain.Purchase
	if err := json.Unmarshal(message, &purchase); err != nil {
		s.log.Error("failed to unmarshal message", slog.Any("error", err))
		if err = s.dlqProducer.Send(ctx, message, err); err != nil {
			s.log.Error("failed to send message to DLQ", slog.Any("error", err))
			// TODO: maybe shutdownChan <- struct{}{}
			return
		}
	}

	s.addToBatch(ctx, purchase)
}

// Shutdown stop service by writing remaining data
func (s *PurchaseService) Shutdown() {
	s.mu.Lock()
	s.flushTicker.Stop()
	batch := append([]domain.Purchase(nil), s.batch...)
	s.batch = s.batch[:0]
	s.mu.Unlock()

	if len(batch) > 0 {
		s.flushBatch(context.Background(), batch)
	}

	// waiting completion all background goroutines
	s.wg.Wait()
}

// flushBatch sends batch of purchases to repository through pool of workers
func (s *PurchaseService) flushBatch(ctx context.Context, batch []domain.Purchase) {
	s.wg.Add(1)
	go func(ctx context.Context, batch []domain.Purchase) {
		defer s.handleWorkerCleanup()

		// block if the workers are busy
		s.workerPool <- struct{}{}

		if err := s.retryAndFilterFailedBatch(ctx, &batch); err != nil {
			s.handleFinalBatchFailure(ctx, batch, err)
		}
	}(ctx, batch)
}

// addToBatch adds purchase to package and initiates package processing if package size is reached.
func (s *PurchaseService) addToBatch(ctx context.Context, purchase domain.Purchase) {
	s.mu.Lock()
	s.batch = append(s.batch, purchase)

	if len(s.batch) >= s.batchSize {
		batch := append([]domain.Purchase(nil), s.batch...)
		s.batch = s.batch[:0]
		s.mu.Unlock()
		s.flushBatch(ctx, batch)
		return
	}
	s.mu.Unlock()
}

// autoFlush timer initiates package processing if package size is not reached.
func (s *PurchaseService) autoFlush() {
	for range s.flushTicker.C {
		s.mu.Lock()
		if len(s.batch) == 0 {
			s.mu.Unlock()
			continue
		}
		batch := s.batch
		s.batch = nil
		s.mu.Unlock()
		s.flushBatch(context.Background(), batch)
	}
}

func (s *PurchaseService) handleWorkerCleanup() {
	defer func() {
		if r := recover(); r != nil {
			s.log.Error("panic in worker", slog.Any("recover", r))
		}
	}()

	<-s.workerPool
	s.wg.Done()
}

func (s *PurchaseService) retryAndFilterFailedBatch(ctx context.Context, batch *[]domain.Purchase) error {
	return retry.Do(
		func() error {
			return s.saveAndFilterBatch(ctx, batch)
		},
		retry.Attempts(s.retryConf.Attempts),
		retry.Delay(s.retryConf.Delay),
		retry.MaxDelay(s.retryConf.MaxDelay),
		retry.RetryIf(func(err error) bool {
			return reperrors.IsRetryableError(err)
		}),
	)
}

func (s *PurchaseService) saveAndFilterBatch(ctx context.Context, batch *[]domain.Purchase) error {
	failedIDs, err := s.repo.SavePurchasesAndReturnFailedIDs(ctx, *batch)
	if err != nil {
		s.log.Warn("batch save attempt failed",
			slog.Int("failed_count", len(failedIDs)),
			slog.Any("error", err))
	}
	// if some of the records have not been saved - filter them for the next attempt.
	if len(failedIDs) > 0 {
		s.log.Warn("unsaved record IDs",
			slog.Any("IDs", failedIDs))
		*batch = filterByIDs(*batch, failedIDs)
	}

	return err
}

func (s *PurchaseService) handleFinalBatchFailure(ctx context.Context, batch []domain.Purchase, err error) {
	s.log.Error("batch save failed after retries",
		slog.Int("remaining_count", len(batch)),
		slog.Any("final_error", err),
	)

	if len(batch) > 0 {
		s.log.Warn("sending failed records to DLQ", slog.Int("count", len(batch)))

		for _, p := range batch {
			message, err := json.Marshal(p)
			if err != nil {
				s.log.Error("failed to marshal purchase for DLQ",
					slog.Any("error", err),
					slog.Any("purchase_id", p.PurchaseID),
				)
				// TODO: maybe shutdownChan <- struct{}{}
				continue
			}
			if err = s.dlqProducer.Send(ctx, message, err); err != nil {
				s.log.Error("failed to send message to DLQ",
					slog.Any("error", err),
					slog.Any("purchase_id", p.PurchaseID),
				)
				// TODO: maybe shutdownChan <- struct{}{}
				continue
			}
		}
	}
}

func filterByIDs(purchases []domain.Purchase, ids []string) []domain.Purchase {
	idSet := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		idSet[id] = struct{}{}
	}

	idx := 0
	for _, p := range purchases {
		if _, exists := idSet[p.PurchaseID]; exists {
			purchases[idx] = p
			idx++
		}
	}

	return purchases[:idx]
}
