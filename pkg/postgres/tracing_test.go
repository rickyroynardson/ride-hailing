package postgres

import "testing"

func TestSpanName(t *testing.T) {
	cases := map[string]string{
		"SELECT status FROM rides WHERE id = $1":                                  "SELECT rides",
		"\n\t\tINSERT INTO outbox(kind, payload) VALUES ($1, $2)":                 "INSERT outbox",
		"UPDATE offers SET status = 'expired' WHERE status = 'pending'":           "UPDATE offers",
		"select exists(select 1 from goose_db_version where version_id = $1)":     "SELECT goose_db_version",
		"WITH revoked AS (UPDATE refresh_tokens SET revoked_at = NOW()) SELECT 1": "WITH refresh_tokens",
		"begin": "BEGIN",
		"":      "query",
	}
	for stmt, want := range cases {
		if got := spanName(stmt); got != want {
			t.Errorf("spanName(%q) = %q, want %q", stmt, got, want)
		}
	}
}
