package server

import ridev1 "github.com/rickyroynardson/ride-hailing/proto/gen/ride/v1"

type RideServer struct {
	ridev1.UnimplementedRideServiceServer
}

func NewRideServer() *RideServer {
	return &RideServer{}
}
