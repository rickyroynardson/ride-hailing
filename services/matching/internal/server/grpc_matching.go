package server

import matchingv1 "github.com/rickyroynardson/ride-hailing/proto/gen/matching/v1"

type MatchingServer struct {
	matchingv1.UnimplementedMatchingServiceServer
}

func NewMatchingServer() *MatchingServer {
	return &MatchingServer{}
}
