# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Package Overview

`lib/encoding` is a low-level binary serialization and compression library from VictoriaMetrics. It provides time-series-optimized encoding for int64 arrays (timestamps and values), variable-length integer encoding, and ZSTD/Snappy compression wrappers. It is vendored into VictoriaLogs and heavily used by `lib/logstorage/` (~49 files import it).

Package: `github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding`

## File Layout

- **`encoding.go`** — Core marshaling/unmarshaling of int64 arrays (timestamps + values). Contains `MarshalType` enum and the adaptive encoding pipeline: detects const, delta-const, gauge, or counter patterns and selects the best encoding strategy.
- **`int.go`** — Fixed-width and variable-length integer encoding. Big-endian for fixed types (uint16/32/64), zig-zag + varint for variable types (VarInt64, VarUint64). Also provides pooled slice types (`Int64s`, `Uint64s`, `Uint32s`) via `sync.Pool`.
- **`float.go`** — Pooled `Float64s` slice type via `sync.Pool`.
- **`nearest_delta.go`** — Delta encoding for gauge-like series. Supports lossy encoding via `precisionBits` (trailing zero bits).
- **`nearest_delta2.go`** — Double-delta (delta-of-deltas) encoding for counter-like series. Same `precisionBits` lossy support.
- **`compress.go`** — ZSTD compression/decompression wrappers with Prometheus-style metrics counters.
- **`util.go`** — `IsZstd()` helper (checks magic number `0xFD2FB528`).
- **`zstd/`** — CGO/pure-Go ZSTD abstraction via build tags. CGO variant uses `github.com/valyala/gozstd`; pure variant uses `github.com/klauspost/compress/zstd`. Includes block-level (`Compress`/`Decompress`) and streaming (`Reader`/`Writer`) APIs.
- **`snappy/`** — Snappy decompression wrapper with max size bounds checking.

## Key Patterns

### Append-style API
All marshal functions follow the pattern `func MarshalX(dst []byte, ...) []byte` — they append to `dst` and return the extended slice. Unmarshal functions similarly append to `dst` slices. This avoids allocations and is the dominant pattern throughout VictoriaMetrics.

### MarshalType Encoding Pipeline
`marshalInt64Array` in `encoding.go` implements adaptive encoding selection:
1. **Const** (`MarshalTypeConst`) — all values identical, stores only `firstValue`
2. **DeltaConst** (`MarshalTypeDeltaConst`) — constant delta between values, stores `firstValue` + delta
3. **NearestDelta** (types 4/6) — gauge-like data, delta-encoded with optional ZSTD
4. **NearestDelta2** (types 1/5) — counter-like data, double-delta-encoded with optional ZSTD

ZSTD variants (types 1, 4) fall back to uncompressed variants (types 5, 6) when compression ratio is worse than 90%.

### Zig-zag Encoding
Signed integers use zig-zag encoding (`(v << 1) ^ (v >> 63)`) for both fixed-width `MarshalInt64` and variable-length `MarshalVarInt64`. This maps small-magnitude signed values to small unsigned values for better varint compression.

### CGO Build Tags
The `zstd/` subpackage uses build tags (`cgo` / `!cgo`) to select between `gozstd` (CGO, faster) and `klauspost/compress/zstd` (pure Go, portable). Both expose identical APIs. The pure-Go variant caches encoders/decoders in atomic maps keyed by compression level.

### sync.Pool Usage
Pooled types (`Int64s`, `Uint64s`, `Uint32s`, `Float64s`) use `GetX(size)`/`PutX()` patterns. Pool contents are uninitialized — callers must not assume zeroed memory.

## Testing and Building

This is vendored code — it cannot be tested or built in isolation from within VictoriaLogs. The canonical tests live in the upstream VictoriaMetrics repository. Changes here should be done via `make vendor-update` at the VictoriaLogs repo root, not by editing vendored files directly.

## Dependencies

- `lib/bytesutil` — `ByteBufferPool` for temporary buffers
- `lib/decimal` — `ExtendInt64sCapacity` for pre-allocation
- `lib/fastnum` — Fast paths for zero/one int64 arrays
- `lib/slicesutil` — `SetLength` for slice capacity management
- `lib/logger` — Panic-on-BUG error handling (internal invariant violations)
- `github.com/VictoriaMetrics/metrics` — Prometheus-compatible counters for compression stats
