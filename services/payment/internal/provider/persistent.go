package provider

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PersistentFake is the development provider used by the service. Like a real provider it durably remembers both
// successful and refused operations, including their parameters. It is not a payment processor: these separate
// tables only simulate the provider side so restarting the application cannot double-charge or forget a refund.
type PersistentFake struct{ db *pgxpool.Pool }

func NewPersistentFake(db *pgxpool.Pool) *PersistentFake { return &PersistentFake{db: db} }

// operation atomically saves the simulated money movement and provider receipt, then optionally loses the reply.
// A transaction-scoped key lock serializes replicas. A repeat returns the exact saved result; changed parameters
// never become a new operation. SQL errors/unknown commit outcomes are retried with the same key.
func (p *PersistentFake) operation(ctx context.Context, kind, key string, request, result any, run func(pgx.Tx) (bool, error)) error {
	payload, err := json.Marshal(request)
	if err != nil {
		return err
	}
	lost := false
	err = pgx.BeginFunc(ctx, p.db, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, "fake:"+kind+":"+key); err != nil {
			return err
		}
		var same bool
		var saved []byte
		err := tx.QueryRow(ctx, `SELECT request = $3::jsonb, result FROM fake_provider_operations WHERE kind = $1 AND key = $2`, kind, key, payload).Scan(&same, &saved)
		if err == nil {
			if !same {
				return ErrKeyReused
			}
			return json.Unmarshal(saved, result)
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		lost, err = run(tx)
		if err != nil {
			return err
		}
		answer, err := json.Marshal(result)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `INSERT INTO fake_provider_operations(kind, key, request, result) VALUES ($1, $2, $3, $4)`, kind, key, payload, answer)
		return err
	})
	if err == nil && lost {
		return ErrOutcomeUnknown
	}
	return err
}

func (p *PersistentFake) Charge(ctx context.Context, req Request) (Result, error) {
	var res Result
	err := p.operation(ctx, "charge", req.Key, req, &res, func(tx pgx.Tx) (bool, error) {
		switch req.Token {
		case TokenOK, TokenLostResponse, TokenRefundRefused, TokenRefundLost, "":
			res.Reference = "fake_" + req.Key
			_, err := tx.Exec(ctx, `INSERT INTO fake_provider_charges(reference, token, amount, currency) VALUES ($1, $2, $3, $4)`, res.Reference, req.Token, req.Amount, req.Currency)
			return req.Token == TokenLostResponse, err
		case TokenDeclined:
			res.DeclineReason = "card_declined"
		default:
			res.DeclineReason = "invalid_card"
		}
		return false, nil
	})
	return res, err
}

func (p *PersistentFake) Refund(ctx context.Context, req RefundRequest) (RefundResult, error) {
	var res RefundResult
	err := p.operation(ctx, "refund", req.Key, req, &res, func(tx pgx.Tx) (bool, error) {
		var token, currency string
		var amount, refunded int64
		err := tx.QueryRow(ctx, `SELECT token, amount, refunded, currency FROM fake_provider_charges WHERE reference = $1 FOR UPDATE`, req.ChargeReference).Scan(&token, &amount, &refunded, &currency)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			res.FailureReason = "unknown_charge"
		case err != nil:
			return false, err
		case token == TokenRefundRefused:
			res.FailureReason = "refund_refused"
		case currency != req.Currency || req.Amount <= 0 || req.Amount > amount-refunded:
			res.FailureReason = "invalid_amount"
		default:
			res.Reference = "fake_refund_" + req.Key
			_, err = tx.Exec(ctx, `UPDATE fake_provider_charges SET refunded = refunded + $2 WHERE reference = $1`, req.ChargeReference, req.Amount)
			return token == TokenRefundLost, err
		}
		return false, nil
	})
	return res, err
}
