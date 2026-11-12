package observability

import (
	"context"
	"fmt"
	"io"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// TracingConfig configura el exporter.
type TracingConfig struct {
	Exporter     string // none | stdout | otlp
	Writer       io.Writer
	OTLPEndpoint string  // host:port del collector (http)
	SampleRatio  float64 // 0..1 para trazas raíz
	Service      string
}

// SetupTracing configura el TracerProvider global y el propagador W3C
// (traceparent/tracestate + baggage). Devuelve el tracer y un shutdown que
// hace flush de los spans pendientes.
func SetupTracing(ctx context.Context, cfg TracingConfig) (trace.Tracer, func(context.Context) error, error) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	if cfg.Service == "" {
		cfg.Service = "crm-ticket-gateway"
	}
	var exp sdktrace.SpanExporter
	var err error
	switch cfg.Exporter {
	case "", "none":
		tp := noop.NewTracerProvider()
		otel.SetTracerProvider(tp)
		return tp.Tracer(cfg.Service), func(context.Context) error { return nil }, nil
	case "stdout":
		exp, err = stdouttrace.New(stdouttrace.WithWriter(cfg.Writer))
	case "otlp":
		exp, err = otlptracehttp.New(ctx, otlptracehttp.WithEndpoint(cfg.OTLPEndpoint), otlptracehttp.WithInsecure())
	default:
		return nil, nil, fmt.Errorf("unknown trace exporter %q", cfg.Exporter)
	}
	if err != nil {
		return nil, nil, err
	}
	res := resource.NewSchemaless(semconv.ServiceName(cfg.Service))
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.SampleRatio))),
	)
	otel.SetTracerProvider(tp)
	return tp.Tracer(cfg.Service), tp.Shutdown, nil
}

// Inject serializa el contexto de traza en un map (headers del evento).
func Inject(ctx context.Context) map[string]string {
	c := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, c)
	return c
}

// Extract recupera el contexto de traza desde headers de un evento.
func Extract(ctx context.Context, h map[string]string) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(h))
}
