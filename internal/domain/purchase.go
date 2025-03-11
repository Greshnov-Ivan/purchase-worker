package domain

import "time"

type Purchase struct {
	UserID     int       `json:"user_id"`
	TrainerID  int       `json:"trainer_id"`
	PurchaseID string    `json:"purchase_id"`
	Timestamp  time.Time `json:"timestamp"`
	Training   Training  `json:"training"`
	Payment    Payment   `json:"payment"`
}

type Training struct {
	Type          string    `json:"type"`
	TrainingID    int       `json:"training_id"`
	Title         string    `json:"title"`
	Category      string    `json:"category"`
	PriceCents    uint      `json:"price_cents"`
	DiscountCents uint      `json:"discount_cents"`
	SessionDate   time.Time `json:"session_date"`
}

type Payment struct {
	Method      string `json:"method"`
	Status      string `json:"status"`
	AmountCents uint   `json:"amount_cents"`
	Currency    string `json:"currency"`
}
