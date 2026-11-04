// Package database records which migration this service's code needs; pkg/postgres.CheckSchema enforces it.
package database

// SchemaVersion is the newest migration this code needs. TestSchemaVersionIsLatestMigration fails when a
// migration is added without bumping it, so the startup check can't silently fall behind.
const SchemaVersion = 20261104100000
