package postgres

import (
	"context"
	"regexp"
	"strings"

	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5"
)

// queryTracer records a span per query and nothing else. Exposing only the query hooks makes pgx skip the
// prepare, connect and pool-acquire hooks, whose spans doubled every trace without saying anything new; pool
// metrics already cover acquire time.
type queryTracer struct {
	inner *otelpgx.Tracer
}

func newQueryTracer() pgx.QueryTracer {
	return queryTracer{inner: otelpgx.NewTracer(otelpgx.WithSpanNameFunc(spanName))}
}

func (t queryTracer) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	return t.inner.TraceQueryStart(ctx, conn, data)
}

func (t queryTracer) TraceQueryEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryEndData) {
	t.inner.TraceQueryEnd(ctx, conn, data)
}

// tablePattern finds the table a statement works on: the word after FROM, INTO or UPDATE.
var tablePattern = regexp.MustCompile(`(?i)\b(?:from|into|update)\s+([a-z_][a-z0-9_]*)`)

// spanName names a query span "VERB table", e.g. "UPDATE rides" or "SELECT offers", so the timeline reads without
// opening each span; the full statement stays in the span's attributes.
func spanName(stmt string) string {
	fields := strings.Fields(stmt)
	if len(fields) == 0 {
		return "query"
	}
	verb := strings.ToUpper(fields[0])
	if m := tablePattern.FindStringSubmatch(stmt); m != nil {
		return verb + " " + strings.ToLower(m[1])
	}
	return verb
}
