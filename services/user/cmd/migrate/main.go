// Migrate applies the repository's existing SQL migrations for local Compose startup.
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/url"
	"os"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

func main() {
	base, err := url.Parse(os.Getenv("POSTGRES_URL"))
	if err != nil || base.Host == "" || (base.Scheme != "postgres" && base.Scheme != "postgresql") {
		log.Fatal("POSTGRES_URL is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	for _, entry := range []struct{ service, database string }{{"user", "postgres"}, {"ride", "rides"}, {"matching", "matching"}, {"payment", "payments"}} {
		dsn := *base
		dsn.Path = "/" + entry.database
		if err := migrate(ctx, dsn.String(), entry.service); err != nil {
			log.Fatalf("%s migration failed: %v", entry.service, err)
		}
	}
}

func migrate(ctx context.Context, dsn, service string) error {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	provider, err := goose.NewProvider(goose.DialectPostgres, db, os.DirFS("/migrations/"+service))
	if err != nil {
		return err
	}
	results, err := provider.Up(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("%s: applied %d pending migrations\n", service, len(results))
	return nil
}
