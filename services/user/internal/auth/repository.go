package auth

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rickyroynardson/ride-hailing/services/user/internal/user"
)

type AuthRepository struct {
	DB *pgxpool.Pool
}

func NewRepository(dbpool *pgxpool.Pool) *AuthRepository {
	return &AuthRepository{DB: dbpool}
}

func (r *AuthRepository) CreateUser(ctx context.Context, in RegisterInput) (user.User, error) {
	var u user.User

	err := r.DB.QueryRow(ctx,
		`INSERT INTO users(email, password_hash, name, phone) VALUES ($1, $2, $3, NULLIF($4, ''))
		RETURNING id, email, name, COALESCE(phone, ''), created_at`,
		in.Email, in.Password, in.Name, in.Phone,
	).Scan(&u.ID, &u.Email, &u.Name, &u.Phone, &u.CreatedAt)

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return user.User{}, ErrUserExists
	}
	return u, err
}

func (r *AuthRepository) GetUserByEmail(ctx context.Context, email string) (user.User, string, error) {
	var u user.User
	var passwordHash string

	err := r.DB.QueryRow(ctx,
		`SELECT id, email, name, COALESCE(phone, ''), created_at, password_hash FROM users WHERE email = $1`,
		email,
	).Scan(&u.ID, &u.Email, &u.Name, &u.Phone, &u.CreatedAt, &passwordHash)

	if errors.Is(err, pgx.ErrNoRows) {
		return user.User{}, "", ErrInvalidCreds
	}
	return u, passwordHash, err
}

func (r *AuthRepository) IsApprovedDriver(ctx context.Context, userID string) (bool, error) {
	var approved bool
	err := r.DB.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM driver_profiles WHERE user_id = $1 AND status = 'approved')`,
		userID,
	).Scan(&approved)
	return approved, err
}

func (r *AuthRepository) CreateRefreshToken(ctx context.Context, userID, tokenHash string, expiresAt time.Time) error {
	_, err := r.DB.Exec(ctx, "INSERT INTO refresh_tokens(user_id, token, expires_at) VALUES ($1, $2, $3)", userID, tokenHash, expiresAt)
	return err
}

func (r *AuthRepository) RevokeRefreshToken(ctx context.Context, tokenHash string) (user.User, error) {
	var u user.User

	err := r.DB.QueryRow(ctx,
		`WITH revoked AS (
			UPDATE refresh_tokens SET revoked_at = NOW()
			WHERE token = $1 AND revoked_at IS NULL AND expires_at > NOW()
			RETURNING user_id
		)
		SELECT u.id, u.email, u.name, COALESCE(u.phone, ''), u.created_at
		FROM users u JOIN revoked ON u.id = revoked.user_id`,
		tokenHash,
	).Scan(&u.ID, &u.Email, &u.Name, &u.Phone, &u.CreatedAt)

	if errors.Is(err, pgx.ErrNoRows) {
		return user.User{}, ErrInvalidToken
	}
	return u, err
}
