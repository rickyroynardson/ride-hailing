package grpcserver

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/resolver"
)

const (
	dnsRefreshEvery  = 5 * time.Second
	dnsLookupTimeout = 2 * time.Second
	serviceDNSScheme = "servicedns"
)

// NewClient is grpc.NewClient with periodic discovery, bounded connection backoff and unary backpressure.
// It accepts host:port or dns:///host:port. Connections are lazy; callers still provide credentials and tracing.
// Per connection, up to 32 unary calls run and 32 wait; each gets a three-second budget including queue time.
// It adds no RPC retry policy: callers still own safe retries and can provide earlier deadlines.
//
// Why not rely on gRPC's normal DNS refresh? Its DNS resolver normally resolves again only when asked after a
// connection failure. If Compose reuses an old IP for another healthy gRPC service, the connection succeeds but
// every call gets "unknown service". There may never be another connection failure to trigger discovery.
func NewClient(address string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	return NewClientWithLimits(address, defaultClientLimits(), opts...)
}

// NewClientWithLimits overrides NewClient's unary capacity and timeout defaults. Reuse the returned connection;
// creating a connection per request defeats the bulkhead. A zero queue capacity means reject rather than wait.
func NewClientWithLimits(address string, limits ClientLimits, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	return newServiceClientWithLimits(address, &serviceDNSBuilder{lookup: net.DefaultResolver.LookupHost}, limits, opts...)
}

func newServiceClient(address string, builder *serviceDNSBuilder, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	return newServiceClientWithLimits(address, builder, defaultClientLimits(), opts...)
}

func newServiceClientWithLimits(address string, builder *serviceDNSBuilder, limits ClientLimits, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	endpoint, err := serviceEndpoint(address)
	if err != nil {
		return nil, err
	}
	bulkhead, err := newUnaryBulkhead(limits)
	if err != nil {
		return nil, err
	}
	defaults := []grpc.DialOption{
		grpc.WithChainUnaryInterceptor(bulkhead.intercept),
		// Scoped to this connection: no process-wide resolver registration or changes to gRPC's DNS globals.
		grpc.WithResolvers(builder),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff:           backoff.Config{BaseDelay: 200 * time.Millisecond, Multiplier: 1.6, Jitter: .2, MaxDelay: 3 * time.Second},
			MinConnectTimeout: 2 * time.Second,
		}),
	}
	return grpc.NewClient(serviceDNSScheme+":///"+endpoint, append(defaults, opts...)...)
}

// serviceEndpoint deliberately supports only our A/AAAA host:port configuration. Reject unsupported schemes,
// authorities and missing ports rather than silently changing unix/passthrough/SRV/custom-resolver semantics.
func serviceEndpoint(address string) (string, error) {
	if strings.Contains(address, "://") {
		u, err := url.Parse(address)
		if err != nil || u.Scheme != "dns" || u.Host != "" || u.RawQuery != "" || u.Fragment != "" || u.User != nil {
			return "", fmt.Errorf("unsupported service address %q: use host:port or dns:///host:port", address)
		}
		address = strings.TrimPrefix(u.Path, "/")
	}
	host, port, err := net.SplitHostPort(address)
	if err != nil || host == "" || strings.ContainsAny(host, "/?# \t\r\n") {
		return "", fmt.Errorf("invalid service address %q: expected host:port", address)
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 {
		return "", fmt.Errorf("invalid port in service address %q", address)
	}
	return net.JoinHostPort(host, port), nil
}

type serviceDNSBuilder struct {
	lookup func(context.Context, string) ([]string, error)
	// Tests drive refreshes explicitly, without sleeping or changing machine DNS. Production uses a five-second ticker.
	ticks func() (<-chan time.Time, func())
}

func (*serviceDNSBuilder) Scheme() string { return serviceDNSScheme }

func (b *serviceDNSBuilder) Build(target resolver.Target, cc resolver.ClientConn, _ resolver.BuildOptions) (resolver.Resolver, error) {
	host, port, err := net.SplitHostPort(target.Endpoint())
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &serviceDNS{ctx: ctx, cancel: cancel, done: make(chan struct{}), cc: cc, host: host, port: port, builder: b}
	go r.run()
	return r, nil
}

type serviceDNS struct {
	ctx        context.Context
	cancel     context.CancelFunc
	done       chan struct{}
	cc         resolver.ClientConn
	host, port string
	builder    *serviceDNSBuilder
	last       []resolver.Address // owned only by run
}

// ResolveNow is only a hint. A periodic lookup is already scheduled, even for healthy connections. Ignoring hints
// caps DNS traffic during a connection outage instead of letting every failed subconnection start another lookup.
func (*serviceDNS) ResolveNow(resolver.ResolveNowOptions) {}

func (r *serviceDNS) Close() { r.cancel(); <-r.done }

func (r *serviceDNS) run() {
	defer close(r.done)
	if ip, err := netip.ParseAddr(r.host); err == nil {
		// Literal IP targets cannot discover replacements. No lookup or ticker is needed for them.
		_ = r.cc.UpdateState(resolver.State{Addresses: []resolver.Address{{Addr: net.JoinHostPort(ip.String(), r.port)}}})
		<-r.ctx.Done()
		return
	}
	var ticks <-chan time.Time
	var stop func()
	if r.builder.ticks != nil {
		ticks, stop = r.builder.ticks()
	} else {
		t := time.NewTicker(dnsRefreshEvery)
		ticks, stop = t.C, t.Stop
	}
	defer stop()
	r.resolve()
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticks:
			r.resolve()
		}
	}
}

func (r *serviceDNS) resolve() {
	ctx, cancel := context.WithTimeout(r.ctx, dnsLookupTimeout)
	ips, err := r.builder.lookup(ctx, r.host)
	cancel()
	if r.ctx.Err() != nil {
		return
	}
	if err == nil && len(ips) == 0 {
		err = fmt.Errorf("no addresses")
	}
	addresses := make([]resolver.Address, 0, len(ips))
	if err == nil {
		for _, value := range ips {
			ip, parseErr := netip.ParseAddr(value)
			if parseErr != nil {
				err = fmt.Errorf("invalid IP address %q", value)
				break
			}
			addresses = append(addresses, resolver.Address{Addr: net.JoinHostPort(ip.Unmap().String(), r.port)})
		}
	}
	if err != nil {
		// Do not replace a usable address set with an empty one during a temporary DNS outage. Report the error to
		// gRPC and try again next tick; its connection state/backoff still handles unreachable old addresses.
		r.cc.ReportError(fmt.Errorf("resolve %s: %w", r.host, err))
		return
	}
	// DNS order changes and duplicate A/AAAA records mustn't churn healthy connections.
	slices.SortFunc(addresses, func(a, b resolver.Address) int { return strings.Compare(a.Addr, b.Addr) })
	addresses = slices.Compact(addresses)
	if slices.Equal(addresses, r.last) {
		return
	}
	if err := r.cc.UpdateState(resolver.State{Addresses: addresses}); err == nil {
		r.last = addresses
	}
}
