package auth

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type AuthRepository struct {
	DB *pgxpool.Pool
}

func NewRepository(dbpool *pgxpool.Pool) *AuthRepository {
	return &AuthRepository{DB: dbpool}
}

func (r *AuthRepository) CreateUser(ctx context.Context, in RegisterInput) (User, error) {
	var u User

	err := r.DB.QueryRow(ctx,
		`INSERT INTO users(email, password_hash, name, phone) VALUES ($1, $2, $3, NULLIF($4, ''))
		RETURNING id, email, name, COALESCE(phone, ''), created_at`,
		in.Email, in.Password, in.Name, in.Phone,
	).Scan(&u.ID, &u.Email, &u.Name, &u.Phone, &u.CreatedAt)

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return User{}, ErrUserExists
	}
	return u, err
}

func (r *AuthRepository) GetUserByEmail(ctx context.Context, email string) (User, string, error) {
	var u User
	var passwordHash string

	err := r.DB.QueryRow(ctx,
		`SELECT id, email, name, COALESCE(phone, ''), created_at, password_hash FROM users WHERE email = $1`,
		email,
	).Scan(&u.ID, &u.Email, &u.Name, &u.Phone, &u.CreatedAt, &passwordHash)

	if errors.Is(err, pgx.ErrNoRows) {
		return User{}, "", ErrInvalidCreds
	}
	return u, passwordHash, err
}

func (r *AuthRepository) CreateRefreshToken(ctx context.Context, userID, tokenHash string, expiresAt time.Time) error {
	_, err := r.DB.Exec(ctx, "INSERT INTO refresh_tokens(user_id, token, expires_at) VALUES ($1, $2, $3)", userID, tokenHash, expiresAt)
	return err
}
