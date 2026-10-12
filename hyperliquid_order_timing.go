package ccxt

// AlphaFox fork extension: order diagnostics belong to a single invocation, never
// Options or the shared HTTP client. Callbacks must be short and non-panicking.
import (
	"context"
	"time"
)

const hyperliquidOrderCallKey = "__alphafoxOrderCall"

type hyperliquidOrderCall struct {
	ctx    context.Context
	timing func(string, time.Duration)
}

// AlphafoxOrderTimingSupported is also promoted to the typed Hyperliquid wrapper.
func (this *HyperliquidCore) AlphafoxOrderTimingSupported() bool { return true }

func (this *HyperliquidCore) takeHyperliquidOrderCall(params any) (any, hyperliquidOrderCall) {
	call, _ := this.SafeValue(params, hyperliquidOrderCallKey).(hyperliquidOrderCall)
	ctx := this.SafeValue(params, "alphafoxOrderContext")
	timing := this.SafeValue(params, "alphafoxOrderTiming")
	if ctx != nil {
		var ok bool
		call.ctx, ok = ctx.(context.Context)
		if !ok {
			panic(ArgumentsRequired("hyperliquid alphafoxOrderContext must be context.Context"))
		}
	}
	if timing != nil {
		var ok bool
		call.timing, ok = timing.(func(string, time.Duration))
		if !ok {
			panic(ArgumentsRequired("hyperliquid alphafoxOrderTiming must be func(string, time.Duration)"))
		}
	}
	if ctx != nil || timing != nil || this.SafeValue(params, hyperliquidOrderCallKey) != nil {
		params = this.Omit(params, []any{"alphafoxOrderContext", "alphafoxOrderTiming", hyperliquidOrderCallKey})
	}
	return params, call
}

func (call hyperliquidOrderCall) attach(params any) {
	if call.ctx != nil || call.timing != nil {
		AddElementToObject(params, hyperliquidOrderCallKey, call)
	}
}

func (call hyperliquidOrderCall) start() time.Time {
	if call.timing == nil {
		return time.Time{}
	}
	return time.Now()
}

func (call hyperliquidOrderCall) finish(stage string, start time.Time) {
	if call.timing != nil {
		call.timing(stage, time.Since(start))
	}
}

func (this *HyperliquidCore) createOrdersRequestTimed(orders any, params any, call hyperliquidOrderCall) any {
	defer call.finish("sign", call.start())
	return this.CreateOrdersRequest(orders, params)
}

// Only the CreateOrder/CreateOrders path attaches this private carrier. It is
// removed before rate-limit cost calculation, signing, fetch history, and JSON.
func (this *BaseExchange) takeHyperliquidOrderTransport(path, api, method, params any) (any, hyperliquidOrderCall) {
	if this.Id != "hyperliquid" || path != "exchange" || api != "private" || method != "POST" {
		return params, hyperliquidOrderCall{}
	}
	call, ok := this.SafeValue(params, hyperliquidOrderCallKey).(hyperliquidOrderCall)
	if !ok {
		return params, hyperliquidOrderCall{}
	}
	return this.Omit(params, hyperliquidOrderCallKey), call
}

func (this *BaseExchange) throttleOrderRequest(cost any, call hyperliquidOrderCall) any {
	defer call.finish("throttle", call.start())
	result := <-this.Throttle(cost)
	PanicOnError(result)
	return result
}

func (this *BaseExchange) signOrderTransport(call hyperliquidOrderCall, path, api, method, params, headers, body any) any {
	defer call.finish("serialize", call.start())
	return this.DerivedExchange.Sign(path, api, method, params, headers, body)
}
