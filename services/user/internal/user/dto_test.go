package user

import (
	"errors"
	"testing"
)

func TestRegisterDriverInputValidate(t *testing.T) {
	valid := RegisterDriverInput{LicenseNumber: " sim 123 ", VehiclePlate: "b 1234 xyz", VehicleModel: " Toyota Avanza "}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	if valid.LicenseNumber != "SIM123" || valid.VehiclePlate != "B1234XYZ" || valid.VehicleModel != "Toyota Avanza" {
		t.Fatalf("got %+v", valid)
	}

	invalid := []RegisterDriverInput{
		{LicenseNumber: " ", VehiclePlate: "B1", VehicleModel: "X"},
		{LicenseNumber: "S1", VehiclePlate: "", VehicleModel: "X"},
		{LicenseNumber: "S1", VehiclePlate: "B1", VehicleModel: " "},
	}
	for _, in := range invalid {
		if err := in.Validate(); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("expected ErrInvalidInput for %+v, got %v", in, err)
		}
	}
}
