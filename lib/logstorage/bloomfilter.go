// Package logstorage provides the core storage engine for VictoriaLogs.
//
// ============== Bloom Filter Overview ==============
//
// Bloom filters are probabilistic data structures that allow quick checks
// for "definitely not present" vs "possibly present". In VictoriaLogs,
// they're used to skip reading column values when the search terms
// definitely don't exist in that column.
//
// ============== How Bloom Filters Work ==============
//
// A bloom filter is a bit array where:
//   - Each token is hashed multiple times (bloomFilterHashesCount = 6)
//   - Each hash result sets one bit in the array
//   - To check: hash the query term, verify all bits are set
//
// PROPERTIES:
//   - No false negatives: if bloom says "not present", it's definitely not there
//   - Possible false positives: if bloom says "present", might not be
//   - Compact: 16 bits per token (bloomFilterBitsPerItem)
//
// ============== Usage in Queries ==============
//
// When querying with a text filter like `_msg:contains("error")`:
//  1. Tokenize "error" and compute bloom hashes
//  2. Check block's bloom filter for these hashes
//  3. If any hash missing → skip the block entirely
//  4. If all hashes present → read and check actual values
//
// This dramatically reduces I/O for selective queries.
//
// ============== Column-Specific Bloom Filters ==============
//
// Each non-dict column in a block has its own bloom filter:
//   - _msg column: stored in message_bloom.bin
//   - Other columns: stored in sharded bloom.binN files
//
// Dictionary-encoded columns don't need bloom filters because
// all unique values are stored in the columnHeader itself.
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

// Bloom filter configuration constants.
//
// These values were chosen to balance:
//   - False positive rate (~1-2%)
//   - Memory/disk overhead
//   - Hash computation time

// bloomFilterHashesCount is the number of hash functions used.
// Higher values reduce false positives but increase computation time.
// 6 hashes gives ~1.5% false positive rate with 16 bits per item.
const bloomFilterHashesCount = 6

// bloomFilterBitsPerItem is the number of bits allocated per token.
// 16 bits = 2 bytes per token, giving ~1.5% false positive rate with 6 hashes.
const bloomFilterBitsPerItem = 16

// bloomFilterMarshalTokens creates and marshals a bloom filter for the given tokens.
// This is the main entry point for creating bloom filters during block writes.
func bloomFilterMarshalTokens(dst []byte, tokens []string) []byte {
	bf := getBloomFilter()
	bf.mustInitTokens(tokens)
	dst = bf.marshal(dst)
	putBloomFilter(bf)
	return dst
}

// bloomFilterMarshalHashes creates and marshals a bloom filter from pre-computed hashes.
// Used when hashes are already available (e.g., during merge operations).
func bloomFilterMarshalHashes(dst []byte, hashes []uint64) []byte {
	bf := getBloomFilter()
	bf.mustInitHashes(hashes)
	dst = bf.marshal(dst)
	putBloomFilter(bf)
	return dst
}

// bloomFilter is a bit-set based probabilistic membership filter.
// It uses a slice of uint64 words for efficient bit operations.
type bloomFilter struct {
	// bits is the underlying bit array, stored as 64-bit words.
	// Each word holds 64 bits. Bit N of the logical array is at:
	//   word index = N / 64, bit position = N % 64
	bits []uint64
}

// reset clears the bloom filter for reuse.
// IMPORTANT: Must clear bits to prevent false positives from stale data.
func (bf *bloomFilter) reset() {
	// Clear bits before reusing - pooled filters may have stale data
	// that would cause false positives if not cleared
	clear(bf.bits)
	bf.bits = bf.bits[:0]
}

// marshal appends the bloom filter bits to dst as a byte slice.
// Each uint64 word is written as 8 bytes in little-endian order.
func (bf *bloomFilter) marshal(dst []byte) []byte {
	bits := bf.bits
	for _, word := range bits {
		dst = encoding.MarshalUint64(dst, word)
	}
	return dst
}

// unmarshal reads a bloom filter from a byte slice.
// The slice must have a length that's a multiple of 8.
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

// mustInitTokens initializes the bloom filter with the given string tokens.
// The filter size is calculated based on the number of tokens.
func (bf *bloomFilter) mustInitTokens(tokens []string) {
	// Calculate size: bloomFilterBitsPerItem bits per token, rounded up to 64-bit words
	bitsCount := len(tokens) * bloomFilterBitsPerItem
	wordsCount := (bitsCount + 63) / 64
	bits := slicesutil.SetLength(bf.bits, wordsCount)
	bloomFilterAddTokens(bits, tokens)
	bf.bits = bits
}

// mustInitHashes initializes the bloom filter with pre-computed hash values.
// Used when the hashes have already been computed elsewhere.
func (bf *bloomFilter) mustInitHashes(hashes []uint64) {
	// Same sizing as mustInitTokens, but caller provides hashes directly
	bitsCount := len(hashes) * bloomFilterBitsPerItem
	wordsCount := (bitsCount + 63) / 64
	bits := slicesutil.SetLength(bf.bits, wordsCount)
	bloomFilterAddHashes(bits, hashes)
	bf.bits = bits
}

// bloomFilterAddTokens adds tokens to the bloom filter by hashing each one.
func bloomFilterAddTokens(bits []uint64, tokens []string) {
	hashesCount := len(tokens) * bloomFilterHashesCount
	a := encoding.GetUint64s(hashesCount)
	// Generate bloomFilterHashesCount hashes per token
	a.A = appendTokensHashes(a.A[:0], tokens)
	initBloomFilter(bits, a.A)
	encoding.PutUint64s(a)
}

// bloomFilterAddHashes adds pre-computed hashes to the bloom filter.
// Each hash is re-hashed bloomFilterHashesCount times for the bloom probe sequence.
func bloomFilterAddHashes(bits, hashes []uint64) {
	hashesCount := len(hashes) * bloomFilterHashesCount
	a := encoding.GetUint64s(hashesCount)
	// Re-hash each input hash to generate probe positions
	a.A = appendHashesHashes(a.A[:0], hashes)
	initBloomFilter(bits, a.A)
	encoding.PutUint64s(a)
}

// initBloomFilter sets bits in the filter for each hash value.
// Each hash maps to one bit position in the filter.
func initBloomFilter(bits, hashes []uint64) {
	maxBits := uint64(len(bits)) * 64
	for _, h := range hashes {
		idx := h % maxBits
		i := idx / 64 // word index
		j := idx % 64 // bit position within word
		mask := uint64(1) << j
		w := bits[i]
		if (w & mask) == 0 {
			// Only write if bit not already set (avoids memory write traffic)
			bits[i] = w | mask
		}
	}
}

// appendTokensHashes generates bloom filter hashes for a list of tokens.
// Each token produces bloomFilterHashesCount hash values.
// The returned hashes can be passed to bloomFilter.containsAll().
func appendTokensHashes(dst []uint64, tokens []string) []uint64 {
	dstLen := len(dst)
	hashesCount := len(tokens) * bloomFilterHashesCount

	dst = slicesutil.SetLength(dst, dstLen+hashesCount)
	dst = dst[:dstLen]

	// Use a buffer for efficient hash generation
	var buf [8]byte
	hp := (*uint64)(unsafe.Pointer(&buf[0]))
	for _, token := range tokens {
		// Seed with the token's hash, then generate k probes by incrementing
		*hp = xxhash.Sum64(bytesutil.ToUnsafeBytes(token))
		for i := 0; i < bloomFilterHashesCount; i++ {
			h := xxhash.Sum64(buf[:])
			(*hp)++
			dst = append(dst, h)
		}
	}
	return dst
}

// appendHashesHashes generates bloom filter hashes from pre-computed hashes.
// This is used during merge when hashes have already been computed.
func appendHashesHashes(dst, hashes []uint64) []uint64 {
	dstLen := len(dst)
	hashesCount := len(hashes) * bloomFilterHashesCount

	dst = slicesutil.SetLength(dst, dstLen+hashesCount)
	dst = dst[:dstLen]

	var buf [8]byte
	hp := (*uint64)(unsafe.Pointer(&buf[0]))
	for _, h := range hashes {
		// Use the provided hash as seed, generate k probes
		*hp = h
		for i := 0; i < bloomFilterHashesCount; i++ {
			h := xxhash.Sum64(buf[:])
			(*hp)++
			dst = append(dst, h)
		}
	}
	return dst
}

// containsAll checks if all the given hashes are present in the bloom filter.
// Returns true if all hashes are present (or might be present - false positives possible).
// Returns false if any hash is definitely not present (no false negatives).
func (bf *bloomFilter) containsAll(hashes []uint64) bool {
	bits := bf.bits
	if len(bits) == 0 {
		// Empty bloom filter means "cannot rule out" - return true
		// to maintain compatibility with empty/legacy blocks
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
			// Bit not set - the token is definitely not present
			return false
		}
	}
	// All bits set - tokens might be present (check actual values)
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
