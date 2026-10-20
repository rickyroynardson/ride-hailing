package auth

import (
	"errors"
	"testing"
)

func TestRegisterInputValidate(t *testing.T) {
	valid := RegisterInput{Email: " Foo@X.com ", Password: "password", Name: " Foo "}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	if valid.Email != "foo@x.com" || valid.Name != "Foo" {
		t.Fatalf("got %+v", valid)
	}

	invalid := []RegisterInput{
		{Email: "bad", Password: "password", Name: "Foo"},
		{Email: "foo <foo@x.com>", Password: "password", Name: "Foo"},
		{Email: "foo@x.com", Password: "short", Name: "Foo"},
		{Email: "foo@x.com", Password: "password", Name: " "},
	}
	for _, in := range invalid {
		if err := in.Validate(); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("expected ErrInvalidInput for %+v, got %v", in, err)
		}
	}
}

func TestLoginInputValidate(t *testing.T) {
	valid := LoginInput{Email: " Foo@X.com ", Password: "password"}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	if valid.Email != "foo@x.com" {
		t.Fatalf("got %+v", valid)
	}

	for _, in := range []LoginInput{{Email: " ", Password: "password"}, {Email: "foo@x.com"}} {
		if err := in.Validate(); !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("expected ErrInvalidInput for %+v, got %v", in, err)
		}
	}
}
