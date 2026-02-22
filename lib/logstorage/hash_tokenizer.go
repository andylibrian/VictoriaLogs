package logstorage

import (
	"sync"

	"github.com/cespare/xxhash/v2"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
)

// tokenizeHashes extracts word tokens from a, hashes them, appends hashes to dst and returns the result.
//
// The returned hashes must be passed to bloomFilterMarshalHashes in order to build bloom filters.
// The returned hashes must be passed to appendHashesHashes before being passed to bloomFilter.containsAll.
func tokenizeHashes(dst []uint64, a []string) []uint64 {
	t := getHashTokenizer()
	for i, s := range a {
		if i > 0 && s == a[i-1] {
			// Input arrays are often sorted and may contain duplicates.
			// Skip adjacent duplicates to avoid repeated tokenization work.
			continue
		}
		dst = t.tokenizeString(dst, s)
	}
	putHashTokenizer(t)

	return dst
}

const hashTokenizerBucketsCount = 1024

// hashTokenizer tokenizes strings and deduplicates token hashes within the
// current batch without allocating per-token maps.
//
// It uses fixed-size buckets plus overflow slices for collisions, while bm
// tracks touched buckets so sparse resets can avoid full scans.
type hashTokenizer struct {
	buckets [hashTokenizerBucketsCount]hashTokenizerBucket
	bm      bitmap
}

type hashTokenizerBucket struct {
	v        uint64
	overflow []uint64
}

func (b *hashTokenizerBucket) reset() {
	// do not spend CPU time on clearing v and b.overflow items,
	// since they'll be overwritten with new items.
	b.overflow = b.overflow[:0]
}

func newHashTokenizer() *hashTokenizer {
	var t hashTokenizer
	// Bitmap indices are bucket indices, so both must have the same size.
	t.bm.init(len(t.buckets))
	return &t
}

func (t *hashTokenizer) reset() {
	if t.bm.onesCount() <= len(t.buckets)/4 {
		// Sparse case: reset only buckets that were actually used.
		t.bm.forEachSetBit(func(idx int) bool {
			t.buckets[idx].reset()
			return false
		})
	} else {
		// Dense case: a full scan is cheaper than iterating bitmap words.
		buckets := t.buckets[:]
		for i := range buckets {
			buckets[i].reset()
		}
		t.bm.init(len(t.buckets))
	}
}

func (t *hashTokenizer) tokenizeString(dst []uint64, s string) []uint64 {
	if !isASCII(s) {
		// Slow path - s contains unicode chars
		return t.tokenizeStringUnicode(dst, s)
	}

	// Fast path for ASCII s
	i := 0
	for i < len(s) {
		// Search for the next token.
		start := len(s)
		for i < len(s) {
			if !isTokenChar(s[i]) {
				i++
				continue
			}
			start = i
			i++
			break
		}
		// Search for the end of the token.
		end := len(s)
		for i < len(s) {
			if isTokenChar(s[i]) {
				i++
				continue
			}
			end = i
			i++
			break
		}
		if end <= start {
			break
		}

		// Register the token.
		token := s[start:end]
		if h, ok := t.addToken(token); ok {
			// addToken returns ok=false for duplicates within the current
			// tokenizer session, so dst contains unique token hashes.
			dst = append(dst, h)
		}
	}
	return dst
}

func (t *hashTokenizer) tokenizeStringUnicode(dst []uint64, s string) []uint64 {
	for len(s) > 0 {
		// Search for the next token.
		n := len(s)
		for offset, r := range s {
			if isTokenRune(r) {
				n = offset
				break
			}
		}
		s = s[n:]
		// Search for the end of the token.
		n = len(s)
		for offset, r := range s {
			if !isTokenRune(r) {
				n = offset
				break
			}
		}
		if n == 0 {
			break
		}

		// Register the token
		token := s[:n]
		s = s[n:]
		if h, ok := t.addToken(token); ok {
			// Keep output semantics identical to the ASCII path.
			dst = append(dst, h)
		}
	}
	return dst
}

func (t *hashTokenizer) addToken(token string) (uint64, bool) {
	h := xxhash.Sum64(bytesutil.ToUnsafeBytes(token))
	idx := int(h % uint64(len(t.buckets)))

	b := &t.buckets[idx]
	if !t.bm.isSetBit(idx) {
		// First hash in this bucket - store directly in inline slot.
		b.v = h
		t.bm.setBit(idx)
		return h, true
	}

	if b.v == h {
		// Duplicate hash in the inline slot.
		return h, false
	}
	for _, v := range b.overflow {
		if v == h {
			// Duplicate hash in the collision list.
			return h, false
		}
	}
	// New hash colliding with existing bucket head.
	b.overflow = append(b.overflow, h)
	return h, true
}

func getHashTokenizer() *hashTokenizer {
	v := hashTokenizerPool.Get()
	if v == nil {
		return newHashTokenizer()
	}
	return v.(*hashTokenizer)
}

func putHashTokenizer(t *hashTokenizer) {
	// Reset dedup state before pooling to avoid cross-query contamination.
	t.reset()
	hashTokenizerPool.Put(t)
}

var hashTokenizerPool sync.Pool
