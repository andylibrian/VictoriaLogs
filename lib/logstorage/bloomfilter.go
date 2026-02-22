package logstorage

import (
	"fmt"
	"sync"
	"unsafe"

	"github.com/cespare/xxhash/v2"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/slicesutil"
)

// Bloom filter sizing constants used by logstorage.
//
// They balance false-positive probability and per-block memory footprint.
// The filter remains exact for "definitely missing" checks and may return
// false positives for "possibly present" checks, which are then verified
// against actual encoded values.

// bloomFilterHashesCount is the number of different hashes to use for bloom filter.
const bloomFilterHashesCount = 6

// bloomFilterBitsPerItem is the number of bits to use per each token.
const bloomFilterBitsPerItem = 16

// bloomFilterMarshalTokens appends marshaled bloom filter for tokens to dst and returns the result.
func bloomFilterMarshalTokens(dst []byte, tokens []string) []byte {
	bf := getBloomFilter()
	bf.mustInitTokens(tokens)
	dst = bf.marshal(dst)
	putBloomFilter(bf)
	return dst
}

// bloomFilterMarshalHashes appends marshaled bloom filter for hashes to dst and returns the result.
func bloomFilterMarshalHashes(dst []byte, hashes []uint64) []byte {
	bf := getBloomFilter()
	bf.mustInitHashes(hashes)
	dst = bf.marshal(dst)
	putBloomFilter(bf)
	return dst
}

// bloomFilter stores a bitset represented as 64-bit words.
//
// Each indexed bit corresponds to one of the derived token hashes.
type bloomFilter struct {
	bits []uint64
}

func (bf *bloomFilter) reset() {
	// Clear bits before reusing the slice, since the pool can hand bf to
	// unrelated queries and stale set bits would produce false positives.
	clear(bf.bits)
	bf.bits = bf.bits[:0]
}

// marshal appends marshaled bf to dst and returns the result.
func (bf *bloomFilter) marshal(dst []byte) []byte {
	bits := bf.bits
	for _, word := range bits {
		dst = encoding.MarshalUint64(dst, word)
	}
	return dst
}

// unmarshal unmarshals bf from src.
func (bf *bloomFilter) unmarshal(src []byte) error {
	if len(src)%8 != 0 {
		return fmt.Errorf("cannot unmarshal bloomFilter from src with size not multiple by 8; len(src)=%d", len(src))
	}
	bf.reset()
	wordsCount := len(src) / 8
	bits := slicesutil.SetLength(bf.bits, wordsCount)
	for i := range bits {
		bits[i] = encoding.UnmarshalUint64(src)
		src = src[8:]
	}
	bf.bits = bits
	return nil
}

// mustInitTokens initializes bf with the given tokens
func (bf *bloomFilter) mustInitTokens(tokens []string) {
	// Allocate just enough words to keep bloomFilterBitsPerItem per token.
	bitsCount := len(tokens) * bloomFilterBitsPerItem
	wordsCount := (bitsCount + 63) / 64
	bits := slicesutil.SetLength(bf.bits, wordsCount)
	bloomFilterAddTokens(bits, tokens)
	bf.bits = bits
}

// mustInitHashes initializes bf with the given hashes
func (bf *bloomFilter) mustInitHashes(hashes []uint64) {
	// The same sizing rule as mustInitTokens(), but caller already provides
	// precomputed token hashes.
	bitsCount := len(hashes) * bloomFilterBitsPerItem
	wordsCount := (bitsCount + 63) / 64
	bits := slicesutil.SetLength(bf.bits, wordsCount)
	bloomFilterAddHashes(bits, hashes)
	bf.bits = bits
}

// bloomFilterAddTokens adds the given tokens to the bloom filter bits
func bloomFilterAddTokens(bits []uint64, tokens []string) {
	hashesCount := len(tokens) * bloomFilterHashesCount
	a := encoding.GetUint64s(hashesCount)
	// Expand each logical token into bloomFilterHashesCount probe hashes.
	a.A = appendTokensHashes(a.A[:0], tokens)
	initBloomFilter(bits, a.A)
	encoding.PutUint64s(a)
}

// bloomFilterAddHashes adds the given hashes to the bloom filter bits.
func bloomFilterAddHashes(bits, hashes []uint64) {
	hashesCount := len(hashes) * bloomFilterHashesCount
	a := encoding.GetUint64s(hashesCount)
	// Re-hash every incoming hash in the same way as appendTokensHashes(),
	// so caller and filter generation use identical probe positions.
	a.A = appendHashesHashes(a.A[:0], hashes)
	initBloomFilter(bits, a.A)
	encoding.PutUint64s(a)
}

func initBloomFilter(bits, hashes []uint64) {
	maxBits := uint64(len(bits)) * 64
	for _, h := range hashes {
		idx := h % maxBits
		i := idx / 64
		j := idx % 64
		mask := uint64(1) << j
		w := bits[i]
		if (w & mask) == 0 {
			// Avoid rewriting already-set bits to keep writes minimal.
			bits[i] = w | mask
		}
	}
}

// appendTokensHashes appends hashes for the given tokens to dst and returns the result.
//
// The appended hashes can be then passed to bloomFilter.containsAll().
func appendTokensHashes(dst []uint64, tokens []string) []uint64 {
	dstLen := len(dst)
	hashesCount := len(tokens) * bloomFilterHashesCount

	dst = slicesutil.SetLength(dst, dstLen+hashesCount)
	dst = dst[:dstLen]

	var buf [8]byte
	hp := (*uint64)(unsafe.Pointer(&buf[0]))
	for _, token := range tokens {
		// Seed with token hash and derive k probes by hashing incremented seeds.
		// This avoids re-allocations and keeps probe generation deterministic.
		*hp = xxhash.Sum64(bytesutil.ToUnsafeBytes(token))
		for i := 0; i < bloomFilterHashesCount; i++ {
			h := xxhash.Sum64(buf[:])
			(*hp)++
			dst = append(dst, h)
		}
	}
	return dst
}

// appendHashesHashes appends hashes for the given hashes to dst and returns the result.
//
// The hashes must be generated from tokens by tokenizeHashes().
// See also appendTokensHashes().
//
// The appended hashes can be then passed to bloomFilter.containsAll().
func appendHashesHashes(dst, hashes []uint64) []uint64 {
	dstLen := len(dst)
	hashesCount := len(hashes) * bloomFilterHashesCount

	dst = slicesutil.SetLength(dst, dstLen+hashesCount)
	dst = dst[:dstLen]

	var buf [8]byte
	hp := (*uint64)(unsafe.Pointer(&buf[0]))
	for _, h := range hashes {
		// Use the provided hash as the seed and derive k probes exactly like
		// appendTokensHashes(), so both code paths stay compatible.
		*hp = h
		for i := 0; i < bloomFilterHashesCount; i++ {
			h := xxhash.Sum64(buf[:])
			(*hp)++
			dst = append(dst, h)
		}
	}
	return dst
}

// containsAll returns true if bf contains all the given tokens hashes generated by appendTokensHashes or appendHashesHashes
func (bf *bloomFilter) containsAll(hashes []uint64) bool {
	bits := bf.bits
	if len(bits) == 0 {
		// Empty bloom filter means "cannot rule out", which keeps compatibility
		// with empty/legacy blocks.
		return true
	}
	maxBits := uint64(len(bits)) * 64
	for _, h := range hashes {
		idx := h % maxBits
		i := idx / 64
		j := idx % 64
		mask := uint64(1) << j
		w := bits[i]
		if (w & mask) == 0 {
			// The token is missing
			return false
		}
	}
	return true
}

func getBloomFilter() *bloomFilter {
	v := bloomFilterPool.Get()
	if v == nil {
		return &bloomFilter{}
	}
	return v.(*bloomFilter)
}

func putBloomFilter(bf *bloomFilter) {
	bf.reset()
	bloomFilterPool.Put(bf)
}

var bloomFilterPool sync.Pool
