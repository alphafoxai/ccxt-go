package ccxt

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const hyperliquidTimingSymbol = "BTC/USDC:USDC"
const hyperliquidTimingReply = `{"status":"ok","response":{"type":"order","data":{"statuses":[{"resting":{"oid":12345}}]}}}`

type hyperliquidTimingContextKey struct{}

type hyperliquidTimingLog struct {
	mu     sync.Mutex
	stages []string
	values []time.Duration
}

func (log *hyperliquidTimingLog) record(stage string, duration time.Duration) {
	log.mu.Lock()
	defer log.mu.Unlock()
	log.stages = append(log.stages, stage)
	log.values = append(log.values, duration)
}

func (log *hyperliquidTimingLog) assert(t *testing.T, want string, throttled bool) {
	t.Helper()
	log.mu.Lock()
	defer log.mu.Unlock()
	if got := strings.Join(log.stages, ","); got != want {
		t.Errorf("timing stages = %s, want %s", got, want)
	}
	for i, duration := range log.values {
		if duration < 0 {
			t.Errorf("negative %s duration: %v", log.stages[i], duration)
		}
		if log.stages[i] == "throttle" && !throttled && duration != 0 {
			t.Errorf("disabled throttle duration = %v, want known zero", duration)
		}
		if log.stages[i] == "throttle" && throttled && duration <= 0 {
			t.Errorf("enabled throttle duration = %v, want actual wait", duration)
		}
	}
}

func newHyperliquidOrderTimingCore(t *testing.T) *HyperliquidCore {
	t.Helper()
	core := newHyperliquidInitTestCore(t, func(string) string { panic("test must install local transport") })
	core.httpClient.Timeout = 0 // keep identity assertions independent of Client's timeout wrapper
	market := core.SafeMarketStructure(map[string]any{
		"id": "0", "baseId": "0", "baseName": "BTC", "symbol": hyperliquidTimingSymbol,
		"base": "BTC", "quote": "USDC", "settle": "USDC",
		"type": "swap", "spot": false, "swap": true, "future": false,
		"option": false, "contract": true, "linear": true, "inverse": false,
		"active": true, "contractSize": 1,
		"precision": map[string]any{"amount": 0.001, "price": 1.0},
	})
	core.Markets = &sync.Map{}
	core.Markets.Store(hyperliquidTimingSymbol, market)
	core.Markets_by_id = &sync.Map{}
	core.Markets_by_id.Store("0", []any{market})
	core.MarketsById = core.Markets_by_id
	core.Options.Store("refSet", true)
	core.Options.Store("builderFeeAutoApprove", false)
	core.Options.Store("enableUnifiedMargin", false)
	if got := awaitHyperliquidInitialization(t, core.InitializeClient()); got != true {
		t.Fatalf("local initialization = %v", got)
	}
	if !NewHyperliquidFromCore(core).AlphafoxOrderTimingSupported() {
		t.Fatal("typed wrapper must expose native capability")
	}
	return core
}

func hyperliquidTimingResponse(r *http.Request) *http.Response {
	return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(hyperliquidTimingReply)), Request: r}
}

func hyperliquidTimingOrders() []any {
	return []any{map[string]any{"symbol": hyperliquidTimingSymbol, "type": "limit", "side": "buy", "amount": 0.001, "price": 50000, "params": map[string]any{}}}
}

func TestHyperliquidOrderTimingConcurrentContexts(t *testing.T) {
	core := newHyperliquidOrderTimingCore(t)
	const callers = 8
	entered := make(chan int, callers)
	release := make(chan struct{})
	defer close(release)
	logs := make([]hyperliquidTimingLog, callers)
	core.httpClient.Transport = hyperliquidInitTransport(func(r *http.Request) (*http.Response, error) {
		id, ok := r.Context().Value(hyperliquidTimingContextKey{}).(int)
		if !ok || id < 0 || id >= callers {
			return nil, fmt.Errorf("missing or crossed order context: %v", id)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			return nil, err
		}
		if bytes.Contains(body, []byte("alphafox")) || !bytes.Contains(body, []byte(fmt.Sprintf("0x%032x", id+1))) {
			t.Errorf("context %d did not match isolated wire order: %s", id, body)
		}
		if !bytes.Contains(body, []byte(`"signature":{"r":`)) {
			t.Error("native signature missing")
		}
		logs[id].assert(t, "sign,throttle,serialize", false)
		entered <- id
		select {
		case <-release:
		case <-r.Context().Done():
			return nil, r.Context().Err()
		}
		return hyperliquidTimingResponse(r), nil
	})
	results := make([]<-chan any, callers)
	for i := range results {
		ctx := context.WithValue(context.Background(), hyperliquidTimingContextKey{}, i)
		params := map[string]any{
			"clientOrderId":        fmt.Sprintf("0x%032x", i+1),
			"alphafoxOrderContext": ctx, "alphafoxOrderTiming": logs[i].record,
		}
		results[i] = core.CreateOrder(hyperliquidTimingSymbol, "limit", "buy", 0.001, 50000, params)
	}
	seen := make(map[int]bool)
	for range results {
		select {
		case id := <-entered:
			if seen[id] {
				t.Fatalf("context %d reused across concurrent calls", id)
			}
			seen[id] = true
		case <-time.After(3 * time.Second):
			t.Fatal("concurrent signed orders did not reach local transport")
		}
	}
	// Separate release channel closure from deferred cleanup on a failed assertion.
	for range callers {
		release <- struct{}{}
	}
	for _, ch := range results {
		if result := awaitHyperliquidInitialization(t, ch); IsError(result) {
			t.Fatalf("native create failed: %v", result)
		}
	}
}

func TestHyperliquidOrderTimingWireAndRetryUnchanged(t *testing.T) {
	core := newHyperliquidOrderTimingCore(t)
	// Exercise the base retry branch with its exact OperationFailed class. The
	// existing Go IsInstance helper does not infer subclasses from NetworkError.
	core.HttpExceptions["503"] = OperationFailed
	core.Options.Store("maxRetriesOnFailure", 1)
	var log hyperliquidTimingLog
	ctx := context.WithValue(context.Background(), hyperliquidTimingContextKey{}, "retry")
	params := map[string]any{"alphafoxOrderContext": ctx, "alphafoxOrderTiming": log.record}
	clean, call := core.takeHyperliquidOrderCall(params)
	if len(clean.(map[string]any)) != 0 || len(params) != 2 {
		t.Fatal("local parameters must be stripped without mutating caller map")
	}
	request := core.createOrdersRequestTimed(hyperliquidTimingOrders(), clean, call)
	// Freeze the real native nonce and signature, then compare the exact JSON from
	// the ordinary native Sign path with every instrumented HTTP retry.
	expected := GetValue(core.Sign("exchange", "private", "POST", request), "body").(string)
	if !strings.Contains(expected, `"signature":{"r":`) {
		t.Fatal("fixture did not execute native crypto signing")
	}
	call.attach(request)
	var attempts atomic.Int64
	core.httpClient.Transport = hyperliquidInitTransport(func(r *http.Request) (*http.Response, error) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			return nil, err
		}
		if string(body) != expected || r.Context().Value(hyperliquidTimingContextKey{}) != "retry" {
			t.Error("retry changed signed wire body or lost per-call context")
		}
		if attempts.Add(1) == 1 {
			return &http.Response{StatusCode: 503, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{}`)), Request: r}, nil
		}
		return hyperliquidTimingResponse(r), nil
	})
	if result := awaitHyperliquidInitialization(t, core.PrivatePostExchange(request)); IsError(result) {
		t.Fatalf("existing network retry did not recover: %v", result)
	}
	if attempts.Load() != 2 {
		t.Fatalf("attempts = %d, want 2", attempts.Load())
	}
	log.assert(t, "sign,throttle,serialize,serialize", false)
}

func TestHyperliquidOrderTimingCancellationClassification(t *testing.T) {
	core := newHyperliquidOrderTimingCore(t)
	core.Options.Store("maxRetriesOnFailure", 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered := make(chan struct{}, 2)
	var log hyperliquidTimingLog
	var attempts atomic.Int64
	core.httpClient.Transport = hyperliquidInitTransport(func(r *http.Request) (*http.Response, error) {
		attempts.Add(1)
		if r.Context() != ctx {
			t.Error("cancellation context was replaced")
		}
		entered <- struct{}{}
		<-r.Context().Done()
		return nil, r.Context().Err()
	})
	result := core.CreateOrders(hyperliquidTimingOrders(), map[string]any{"alphafoxOrderContext": ctx, "alphafoxOrderTiming": log.record})
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("order did not enter local transport")
	}
	cancel()
	got := awaitHyperliquidInitialization(t, result)
	if !IsError(got) || !strings.Contains(ToString(got), "NetworkError") || !strings.Contains(ToString(got), "context canceled") {
		t.Fatalf("cancellation lost existing recoverable network classification: %v", got)
	}
	if attempts.Load() != 1 {
		t.Fatalf("cancellation attempts = %d, want unchanged non-retryable NetworkError policy", attempts.Load())
	}
	log.assert(t, "sign,throttle,serialize", false)
}

func TestHyperliquidOrderTimingActualThrottleAndSigningFailure(t *testing.T) {
	t.Run("actual throttle", func(t *testing.T) {
		core := newHyperliquidOrderTimingCore(t)
		core.EnableRateLimit = true
		core.httpClient.Transport = hyperliquidInitTransport(func(r *http.Request) (*http.Response, error) {
			return hyperliquidTimingResponse(r), nil
		})
		var log hyperliquidTimingLog
		got := awaitHyperliquidInitialization(t, core.CreateOrders(hyperliquidTimingOrders(), map[string]any{"alphafoxOrderTiming": log.record}))
		if IsError(got) {
			t.Fatalf("throttled native order failed: %v", got)
		}
		log.assert(t, "sign,throttle,serialize", true)
	})
	t.Run("sign failure", func(t *testing.T) {
		core := newHyperliquidOrderTimingCore(t)
		core.PrivateKey = ""
		var log hyperliquidTimingLog
		got := awaitHyperliquidInitialization(t, core.CreateOrders(hyperliquidTimingOrders(), map[string]any{"alphafoxOrderTiming": log.record}))
		if !IsError(got) {
			t.Fatalf("missing credentials unexpectedly signed: %v", got)
		}
		log.assert(t, "sign", false)
	})
}

func TestHyperliquidOrderTimingDoesNotAttachToInitialization(t *testing.T) {
	core := newHyperliquidOrderTimingCore(t)
	core.Options.Store("refSet", false)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var log hyperliquidTimingLog
	var initCalls atomic.Int64
	core.httpClient.Transport = hyperliquidInitTransport(func(r *http.Request) (*http.Response, error) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			return nil, err
		}
		if bytes.Contains(body, []byte(`"type":"order"`)) {
			if r.Context() != ctx {
				t.Error("order did not receive caller context")
			}
			return nil, r.Context().Err()
		}
		initCalls.Add(1)
		if r.Context().Err() != nil || bytes.Contains(body, []byte("alphafox")) {
			t.Error("shared initialization inherited order context or callbacks")
		}
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{"status":"ok","response":{"type":"default"}}`)), Request: r}, nil
	})
	got := awaitHyperliquidInitialization(t, core.CreateOrder(hyperliquidTimingSymbol, "limit", "buy", 0.001, 50000, map[string]any{"alphafoxOrderContext": ctx, "alphafoxOrderTiming": log.record}))
	if !IsError(got) || !strings.Contains(ToString(got), "NetworkError") || initCalls.Load() != 1 {
		t.Fatalf("initialization/order context isolation failed: result=%v initCalls=%d", got, initCalls.Load())
	}
	log.assert(t, "sign,throttle,serialize", false)
}

func TestHyperliquidOrderTimingUnknownOutcomeClassification(t *testing.T) {
	core := newHyperliquidOrderTimingCore(t)
	core.Options.Store("maxRetriesOnFailure", 1)
	var attempts atomic.Int64
	var log hyperliquidTimingLog
	core.httpClient.Transport = hyperliquidInitTransport(func(r *http.Request) (*http.Response, error) {
		attempts.Add(1)
		return nil, errors.New("local unknown transport outcome")
	})
	got := awaitHyperliquidInitialization(t, core.CreateOrders(hyperliquidTimingOrders(), map[string]any{"alphafoxOrderTiming": log.record}))
	if !IsError(got) || !strings.Contains(ToString(got), "NetworkError") || !strings.Contains(ToString(got), "local unknown transport outcome") {
		t.Fatalf("unknown outcome lost network classification: %v", got)
	}
	if attempts.Load() != 1 {
		t.Fatalf("unknown outcome attempts = %d, want unchanged native policy", attempts.Load())
	}
	log.assert(t, "sign,throttle,serialize", false)
}
