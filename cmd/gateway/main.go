// Command gateway levanta el servicio de tickets.
package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/federiconbaez/cortex9-crm-ticket-gateway-go/internal/app"
)

func main() {
	cfg, err := app.FromEnv(nil)
	lvl := slog.LevelInfo
	_ = lvl.UnmarshalText([]byte(cfg.LogLevel))
	log := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: lvl}))
	if err != nil {
		log.Error("config", "err", err)
		os.Exit(2)
	}
	a, err := app.New(cfg, log)
	if err != nil {
		log.Error("init", "err", err)
		os.Exit(1)
	}
	ln, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		log.Error("listen", "err", err)
		os.Exit(1)
	}
	adminLn, err := net.Listen("tcp", cfg.AdminAddr)
	if err != nil {
		log.Error("listen admin", "err", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := a.Serve(ctx, ln, adminLn); err != nil {
		log.Error("serve", "err", err)
		os.Exit(1)
	}
}
