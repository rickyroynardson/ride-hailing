package user

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type UserRepository struct {
	DB *pgxpool.Pool
}

func NewRepository(dbpool *pgxpool.Pool) *UserRepository {
	return &UserRepository{DB: dbpool}
}

func (r *UserRepository) GetUserByID(ctx context.Context, id string) (User, error) {
	var u User

	err := r.DB.QueryRow(ctx,
		`SELECT id, email, name, COALESCE(phone, ''), created_at FROM users WHERE id = $1`,
		id,
	).Scan(&u.ID, &u.Email, &u.Name, &u.Phone, &u.CreatedAt)

	if errors.Is(err, pgx.ErrNoRows) {
		return User{}, ErrUserNotFound
	}
	return u, err
}

func (r *UserRepository) CreateDriverProfile(ctx context.Context, userID string, in RegisterDriverInput) (DriverProfile, error) {
	var d DriverProfile

	err := r.DB.QueryRow(ctx,
		`INSERT INTO driver_profiles(user_id, license_number, vehicle_plate, vehicle_model) VALUES ($1, $2, $3, $4)
		RETURNING license_number, vehicle_plate, vehicle_model, status, created_at`,
		userID, in.LicenseNumber, in.VehiclePlate, in.VehicleModel,
	).Scan(&d.LicenseNumber, &d.VehiclePlate, &d.VehicleModel, &d.Status, &d.CreatedAt)

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return DriverProfile{}, ErrDriverExists
	}
	return d, err
}

// GetDriverProfile returns nil when the user is not a driver.
func (r *UserRepository) GetDriverProfile(ctx context.Context, userID string) (*DriverProfile, error) {
	var d DriverProfile

	err := r.DB.QueryRow(ctx,
		`SELECT license_number, vehicle_plate, vehicle_model, status, created_at FROM driver_profiles WHERE user_id = $1`,
		userID,
	).Scan(&d.LicenseNumber, &d.VehiclePlate, &d.VehicleModel, &d.Status, &d.CreatedAt)

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &d, nil
}
