package server_test

import (
	"context"
	"database/sql"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/golang-jwt/jwt/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	userv1 "github.com/rickyroynardson/ride-hailing/proto/gen/user/v1"
	"github.com/rickyroynardson/ride-hailing/services/user/internal/auth"
	"github.com/rickyroynardson/ride-hailing/services/user/internal/server"
	"github.com/rickyroynardson/ride-hailing/services/user/internal/user"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// TestIntegration runs the real gRPC server, interceptors and SQL against a throwaway Postgres container.
// Skipped with -short or when Docker is unavailable.
func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	testcontainers.SkipIfProviderIsNotHealthy(t)
	ctx := context.Background()

	pg, err := tcpostgres.Run(ctx, "postgres:18-alpine", tcpostgres.BasicWaitStrategies())
	testcontainers.CleanupContainer(t, pg)
	if err != nil {
		t.Fatal(err)
	}
	url := pg.MustConnectionString(ctx, "sslmode=disable")

	migrate(t, url)
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	authClient, userClient := startServer(t, pool)

	// Register, login, GetMe.
	reg, err := authClient.Register(ctx, &userv1.RegisterRequest{Email: "Rider@X.com", Password: "password123", Name: "Rider"})
	if err != nil {
		t.Fatal(err)
	}
	if reg.GetUser().GetEmail() != "rider@x.com" {
		t.Fatalf("email not normalized: %q", reg.GetUser().GetEmail())
	}
	_, err = authClient.Register(ctx, &userv1.RegisterRequest{Email: "rider@x.com", Password: "password123", Name: "Dup"})
	wantCode(t, "duplicate register", err, codes.AlreadyExists)

	_, err = authClient.Login(ctx, &userv1.LoginRequest{Email: "rider@x.com", Password: "wrong-password"})
	wantCode(t, "wrong password", err, codes.Unauthenticated)

	login, err := authClient.Login(ctx, &userv1.LoginRequest{Email: "rider@x.com", Password: "password123"})
	if err != nil {
		t.Fatal(err)
	}
	authed := withToken(ctx, login.GetTokens().GetAccessToken())

	_, err = userClient.GetMe(ctx, &userv1.GetMeRequest{})
	wantCode(t, "GetMe without token", err, codes.Unauthenticated)

	me, err := userClient.GetMe(authed, &userv1.GetMeRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if me.GetUser().GetId() != reg.GetUser().GetId() || me.GetDriverProfile() != nil {
		t.Fatalf("GetMe: got %+v", me)
	}

	// Driver registration, approval, and the driver claim after refresh.
	drv, err := userClient.RegisterDriver(authed, &userv1.RegisterDriverRequest{LicenseNumber: "sim 1", VehiclePlate: "b 1 xyz", VehicleModel: "Avanza"})
	if err != nil {
		t.Fatal(err)
	}
	if p := drv.GetDriverProfile(); p.GetStatus() != "pending" || p.GetVehiclePlate() != "B1XYZ" {
		t.Fatalf("RegisterDriver: got %+v", p)
	}
	_, err = userClient.RegisterDriver(authed, &userv1.RegisterDriverRequest{LicenseNumber: "sim 2", VehiclePlate: "b 2", VehicleModel: "Avanza"})
	wantCode(t, "second driver profile", err, codes.AlreadyExists)
	if driverClaim(t, login.GetTokens().GetAccessToken()) {
		t.Fatal("pending driver got driver claim")
	}

	if _, err := pool.Exec(ctx, `UPDATE driver_profiles SET status = 'approved' WHERE user_id = $1`, reg.GetUser().GetId()); err != nil {
		t.Fatal(err)
	}
	refreshed, err := authClient.Refresh(ctx, &userv1.RefreshRequest{RefreshToken: login.GetTokens().GetRefreshToken()})
	if err != nil {
		t.Fatal(err)
	}
	if !driverClaim(t, refreshed.GetTokens().GetAccessToken()) || refreshed.GetUser().GetId() != reg.GetUser().GetId() {
		t.Fatalf("Refresh: got %+v", refreshed)
	}

	// Rotation: the old refresh token is single use.
	_, err = authClient.Refresh(ctx, &userv1.RefreshRequest{RefreshToken: login.GetTokens().GetRefreshToken()})
	wantCode(t, "reused refresh token", err, codes.Unauthenticated)

	// Concurrent refreshes with one token: exactly one may win.
	var wins atomic.Int32
	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() {
			if _, err := authClient.Refresh(ctx, &userv1.RefreshRequest{RefreshToken: refreshed.GetTokens().GetRefreshToken()}); err == nil {
				wins.Add(1)
			}
		})
	}
	wg.Wait()
	if wins.Load() != 1 {
		t.Fatalf("concurrent refresh: %d wins, want 1", wins.Load())
	}

	// Logout is idempotent and kills the refresh token.
	login, err = authClient.Login(ctx, &userv1.LoginRequest{Email: "rider@x.com", Password: "password123"})
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := authClient.Logout(ctx, &userv1.LogoutRequest{RefreshToken: login.GetTokens().GetRefreshToken()}); err != nil {
			t.Fatal(err)
		}
	}
	_, err = authClient.Refresh(ctx, &userv1.RefreshRequest{RefreshToken: login.GetTokens().GetRefreshToken()})
	wantCode(t, "refresh after logout", err, codes.Unauthenticated)
}

func migrate(t *testing.T, url string) {
	t.Helper()
	db, err := sql.Open("pgx", url)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	provider, err := goose.NewProvider(goose.DialectPostgres, db, os.DirFS("../../migrations"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Up(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func startServer(t *testing.T, pool *pgxpool.Pool) (userv1.AuthServiceClient, userv1.UserServiceClient) {
	t.Helper()
	gs := server.NewGRPCServer(zap.NewNop(),
		auth.NewService(auth.NewRepository(pool), "test-secret"),
		user.NewService(user.NewRepository(pool)),
	)
	lis := bufconn.Listen(1 << 20)
	go gs.Serve(lis)
	t.Cleanup(gs.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })

	return userv1.NewAuthServiceClient(conn), userv1.NewUserServiceClient(conn)
}

func withToken(ctx context.Context, access string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+access)
}

func driverClaim(t *testing.T, access string) bool {
	t.Helper()
	claims := jwt.MapClaims{}
	if _, _, err := jwt.NewParser().ParseUnverified(access, claims); err != nil {
		t.Fatal(err)
	}
	driver, _ := claims["driver"].(bool)
	return driver
}

func wantCode(t *testing.T, name string, err error, code codes.Code) {
	t.Helper()
	if got := status.Code(err); got != code {
		t.Fatalf("%s: got %v (%v), want %v", name, got, err, code)
	}
}
