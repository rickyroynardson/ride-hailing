package server

import (
	"context"
	"strings"

	"github.com/rickyroynardson/ride-hailing/services/user/internal/auth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type userIDKey struct{}

func authInterceptor(authService *auth.AuthService) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if strings.HasPrefix(info.FullMethod, "/user.v1.AuthService/") {
			return handler(ctx, req)
		}

		md, _ := metadata.FromIncomingContext(ctx)
		token, ok := strings.CutPrefix(strings.Join(md.Get("authorization"), ""), "Bearer ")
		if !ok {
			return nil, toStatus(auth.ErrInvalidAuth)
		}

		userID, err := authService.VerifyAccessToken(token)
		if err != nil {
			return nil, toStatus(err)
		}

		return handler(context.WithValue(ctx, userIDKey{}, userID), req)
	}
}

func userIDFromContext(ctx context.Context) string {
	id, _ := ctx.Value(userIDKey{}).(string)
	return id
}
