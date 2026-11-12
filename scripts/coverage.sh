#!/usr/bin/env bash
# Cobertura real del código de internal/: -coverpkg hace que los tests de
# integración de internal/app cuenten también para api y observability
# (sin esa bandera, cada paquete solo mide sus propios tests).
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p results
go test -count=1 -coverpkg=./internal/... -coverprofile=results/coverage.out ./internal/... >/dev/null
{
  echo "# go test -coverpkg=./internal/... ./internal/..."
  go tool cover -func=results/coverage.out | awk '/^total/ {print "total:", $NF}'
  echo
  echo "# por paquete, solo con sus propios tests (go test -cover)."
  echo "# api y observability no tienen tests propios: los ejercitan los de internal/app."
  go test -count=1 -cover ./internal/... | awk '/^ok/ {sub(/.*internal\//,"internal/",$2); print $2, $(NF-2)}'
} > results/coverage.txt
rm -f results/coverage.out
cat results/coverage.txt
