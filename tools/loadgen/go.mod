module github.com/rickyroynardson/ride-hailing/tools/loadgen

go 1.27.1

replace github.com/rickyroynardson/ride-hailing/proto => ../../proto

require (
	github.com/jackc/pgx/v5 v5.12.0
	github.com/rickyroynardson/ride-hailing/proto v0.0.0-00010101000000-000000000000
	golang.org/x/sync v0.24.0
	google.golang.org/protobuf v1.36.12
)

require (
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.33.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.3 // indirect
	golang.org/x/net v0.61.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
	golang.org/x/text v0.43.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20261026165753-2ffab32bce7e // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20261019180522-ce908d29299b // indirect
	google.golang.org/grpc v1.84.0 // indirect
)
