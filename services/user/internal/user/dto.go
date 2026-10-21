package user

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

var (
	ErrInvalidInput = errors.New("invalid input")
	ErrUserNotFound = errors.New("user not found")
	ErrDriverExists = errors.New("already registered as driver, or license/plate in use")
)

type User struct {
	ID        string
	Email     string
	Name      string
	Phone     string
	CreatedAt time.Time
}

type DriverProfile struct {
	LicenseNumber string
	VehiclePlate  string
	VehicleModel  string
	Status        string
	CreatedAt     time.Time
}

type RegisterDriverInput struct {
	LicenseNumber string
	VehiclePlate  string
	VehicleModel  string
}

// Validate normalizes license and plate to upper case without spaces, so "b 1234 xyz" and "B1234XYZ" hit the same unique constraint.
func (in *RegisterDriverInput) Validate() error {
	in.LicenseNumber = strings.ToUpper(strings.Join(strings.Fields(in.LicenseNumber), ""))
	in.VehiclePlate = strings.ToUpper(strings.Join(strings.Fields(in.VehiclePlate), ""))
	in.VehicleModel = strings.TrimSpace(in.VehicleModel)

	if in.LicenseNumber == "" || in.VehiclePlate == "" || in.VehicleModel == "" {
		return fmt.Errorf("%w: license number, vehicle plate and vehicle model are required", ErrInvalidInput)
	}

	return nil
}
