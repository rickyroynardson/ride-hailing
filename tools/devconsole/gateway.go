package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// gatewayClient uses the public HTTP API, including its NDJSON streams. Protos are just the JSON schema here, not
// gRPC clients. Reading routes from their annotations keeps the console in step with the gateway's contract.
type gatewayClient struct {
	base string
	http *http.Client
}

func newGatewayClient(base string) *gatewayClient {
	return &gatewayClient{
		base: strings.TrimRight(base, "/"),
		http: &http.Client{Transport: otelhttp.NewTransport(http.DefaultTransport)},
	}
}

func publicRoute(method string, req proto.Message) (verb, path string, output proto.Message, err error) {
	d, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(strings.ReplaceAll(method, "/", ".")))
	if err != nil {
		return "", "", nil, err
	}
	m, ok := d.(protoreflect.MethodDescriptor)
	if !ok {
		return "", "", nil, fmt.Errorf("not an RPC: %s", method)
	}
	rule, ok := proto.GetExtension(m.Options(), annotations.E_Http).(*annotations.HttpRule)
	if !ok || rule == nil {
		return "", "", nil, fmt.Errorf("no public HTTP route for %s", method)
	}
	switch r := rule.Pattern.(type) {
	case *annotations.HttpRule_Get:
		verb, path = http.MethodGet, r.Get
	case *annotations.HttpRule_Post:
		verb, path = http.MethodPost, r.Post
	case *annotations.HttpRule_Put:
		verb, path = http.MethodPut, r.Put
	case *annotations.HttpRule_Delete:
		verb, path = http.MethodDelete, r.Delete
	default:
		return "", "", nil, fmt.Errorf("unsupported HTTP rule for %s", method)
	}
	for strings.Contains(path, "{") {
		start := strings.Index(path, "{")
		end := strings.Index(path[start:], "}") + start
		field := req.ProtoReflect().Descriptor().Fields().ByName(protoreflect.Name(path[start+1 : end]))
		if field == nil {
			return "", "", nil, fmt.Errorf("unknown path field in %s", method)
		}
		value := url.PathEscape(req.ProtoReflect().Get(field).String())
		path = path[:start] + value + path[end+1:]
	}
	t, err := protoregistry.GlobalTypes.FindMessageByName(m.Output().FullName())
	if err != nil {
		return "", "", nil, err
	}
	return verb, path, t.New().Interface(), nil
}

func (g *gatewayClient) request(ctx context.Context, verb, path, token string, in proto.Message) (*http.Response, error) {
	var body io.Reader
	if verb != http.MethodGet && verb != http.MethodDelete {
		b, err := protojson.Marshal(in)
		if err != nil {
			return nil, err
		}
		body = bytes.NewReader(b)
	}
	r, err := http.NewRequestWithContext(ctx, verb, g.base+path, body)
	if err != nil {
		return nil, err
	}
	r.Header.Set("Content-Type", "application/json")
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	return g.http.Do(r)
}

var decodeJSON = protojson.UnmarshalOptions{DiscardUnknown: true}

func (g *gatewayClient) call(ctx context.Context, method, token string, in proto.Message) (out proto.Message, verb, path string, httpStatus int, err error) {
	verb, path, out, err = publicRoute(method, in)
	if err != nil {
		return
	}
	res, err := g.request(ctx, verb, path, token, in)
	if err != nil {
		return nil, verb, path, 0, err
	}
	defer res.Body.Close()
	b, err := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	if err == nil {
		if res.StatusCode/100 != 2 {
			err = httpError(res.StatusCode, b)
		} else {
			err = decodeJSON.Unmarshal(b, out)
		}
	}
	return out, verb, path, res.StatusCode, err
}

// httpError retains the gateway's gRPC error code for the call log, and its HTTP status in the visible detail.
func httpError(httpStatus int, body []byte) error {
	var e struct {
		Code    codes.Code `json:"code"`
		Message string     `json:"message"`
	}
	if json.Unmarshal(body, &e) != nil || e.Code == codes.OK {
		e.Code, e.Message = codes.Unknown, http.StatusText(httpStatus)
	}
	return &gatewayError{httpStatus: httpStatus, cause: status.Errorf(e.Code, "HTTP %d: %s", httpStatus, e.Message)}
}

type gatewayError struct {
	httpStatus int
	cause      error
}

func (e *gatewayError) Error() string              { return e.cause.Error() }
func (e *gatewayError) GRPCStatus() *status.Status { return status.Convert(e.cause) }

// watch reads each result as soon as it is flushed, and returns on EOF, a stream error, cancellation or onMessage=false.
// Its caller reconnects with a fresh token. A bounded lifetime also refreshes tokens on a quiet stream.
func (g *gatewayClient) watch(ctx context.Context, method, token string, in proto.Message, connected func(), onMessage func(proto.Message) bool) error {
	ctx, cancel := context.WithTimeout(ctx, 12*time.Minute)
	defer cancel()
	verb, path, out, err := publicRoute(method, in)
	if err != nil {
		return err
	}
	res, err := g.request(ctx, verb, path, token, in)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode/100 != 2 {
		b, _ := io.ReadAll(io.LimitReader(res.Body, 1<<20))
		return httpError(res.StatusCode, b)
	}
	connected()
	s := bufio.NewScanner(res.Body)
	s.Buffer(make([]byte, 4096), 1<<20)
	for s.Scan() {
		var frame struct {
			Result json.RawMessage `json:"result"`
			Error  json.RawMessage `json:"error"`
		}
		if err := json.Unmarshal(s.Bytes(), &frame); err != nil {
			return err
		}
		if len(frame.Error) > 0 && string(frame.Error) != "null" {
			return httpError(http.StatusOK, frame.Error)
		}
		m := out.ProtoReflect().New().Interface()
		if err := decodeJSON.Unmarshal(frame.Result, m); err != nil {
			return err
		}
		if !onMessage(m) {
			return nil
		}
	}
	if err := s.Err(); err != nil {
		return err
	}
	return io.EOF
}
