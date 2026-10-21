package auth

import (
	"errors"
	"fmt"
	"net/mail"
	"strings"
	"time"
)

var (
	ErrInvalidInput = errors.New("invalid input")
	ErrUserExists   = errors.New("email or phone already registered")
	ErrInvalidCreds = errors.New("invalid email or password")
	ErrInvalidToken = errors.New("invalid or expired refresh token")
	ErrInvalidAuth  = errors.New("invalid or missing access token")
	ErrUserNotFound = errors.New("user not found")
)

type User struct {
	ID        string
	Email     string
	Name      string
	Phone     string
	CreatedAt time.Time
}

type Tokens struct {
	Access  string
	Refresh string
}

type RegisterInput struct {
	Email    string
	Password string
	Name     string
	Phone    string
}

func (in *RegisterInput) Validate() error {
	in.Email = strings.ToLower(strings.TrimSpace(in.Email))
	in.Name = strings.TrimSpace(in.Name)
	in.Phone = strings.TrimSpace(in.Phone)

	if addr, err := mail.ParseAddress(in.Email); err != nil || addr.Address != in.Email {
		return fmt.Errorf("%w: invalid email", ErrInvalidInput)
	}
	if len(in.Password) < 8 || len(in.Password) > 72 {
		return fmt.Errorf("%w: password must be 8-72 characters", ErrInvalidInput)
	}
	if in.Name == "" {
		return fmt.Errorf("%w: name is required", ErrInvalidInput)
	}

	return nil
}

type LoginInput struct {
	Email    string
	Password string
}

func (in *LoginInput) Validate() error {
	in.Email = strings.ToLower(strings.TrimSpace(in.Email))

	if in.Email == "" || in.Password == "" {
		return fmt.Errorf("%w: email and password are required", ErrInvalidInput)
	}

	return nil
}
