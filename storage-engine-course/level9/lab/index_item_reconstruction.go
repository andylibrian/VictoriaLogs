package main

import (
	"encoding/hex"
	"fmt"
	"strings"
)

// ==================== Index Types ====================

type TenantID struct {
	AccountID uint32
	ProjectID uint32
}

func (t *TenantID) Marshal(dst []byte) []byte {
	dst = append(dst, byte(t.AccountID>>24), byte(t.AccountID>>16), byte(t.AccountID>>8), byte(t.AccountID))
	dst = append(dst, byte(t.ProjectID>>24), byte(t.ProjectID>>16), byte(t.ProjectID>>8), byte(t.ProjectID))
	return dst
}

func (t *TenantID) String() string {
	return fmt.Sprintf("{AccountID:%d,ProjectID:%d}", t.AccountID, t.ProjectID)
}

type StreamID struct {
	Hi uint64
	Lo uint64
}

func (s *StreamID) Marshal(dst []byte) []byte {
	for i := 7; i >= 0; i-- {
		dst = append(dst, byte(s.Hi>>(i*8)))
	}
	for i := 7; i >= 0; i-- {
		dst = append(dst, byte(s.Lo>>(i*8)))
	}
	return dst
}

func (s *StreamID) String() string {
	return fmt.Sprintf("%016X_%016X", s.Hi, s.Lo)
}

type StreamTag struct {
	Name  string
	Value string
}

func (t *StreamTag) Marshal(dst []byte) []byte {
	dst = append(dst, byte(len(t.Name)))
	dst = append(dst, t.Name...)
	dst = append(dst, 0xFF) // tagSeparatorChar
	dst = append(dst, byte(len(t.Value)))
	dst = append(dst, t.Value...)
	return dst
}

// ==================== Namespace Prefixes ====================

const (
	nsPrefixStreamID             = 0x00
	nsPrefixStreamIDToStreamTags = 0x01
	nsPrefixTagToStreamIDs       = 0x02
)

// ==================== Index Item Construction ====================

func IndexItemReconstructionDemo() {
	fmt.Println("=== Level 9 Lab 1: Index Item Reconstruction ===")
	fmt.Println()

	// Input
	tenantID := TenantID{AccountID: 12345, ProjectID: 0}
	streamID := StreamID{Hi: 0x0000000000000001, Lo: 0x00000000000000AB}
	tags := []StreamTag{
		{Name: "app", Value: "nginx"},
		{Name: "host", Value: "web-01"},
		{Name: "env", Value: "prod"},
	}

	fmt.Println("Input:")
	fmt.Printf("  TenantID:   %s\n", &tenantID)
	fmt.Printf("  StreamID:   %s\n", &streamID)
	fmt.Printf("  StreamTags: %s\n\n", formatTags(tags))

	// Construct items
	fmt.Println("=== Namespace Prefixes ===")
	fmt.Println()
	fmt.Println("  nsPrefixStreamID             = 0x00 (Stream existence)")
	fmt.Println("  nsPrefixStreamIDToStreamTags = 0x01 (Stream ID → tags)")
	fmt.Println("  nsPrefixTagToStreamIDs       = 0x02 (Tag → stream IDs)")
	fmt.Println()

	// Item 1: Stream existence
	fmt.Println("--- Item 1: Stream Existence Marker ---")
	fmt.Println()
	item1 := constructStreamExistenceItem(&tenantID, &streamID)
	printItem("Purpose", "Fast existence check - 'Does this stream exist?'")
	printItem("Format", "[0x00][tenantID 8B][streamID 16B]")
	printItem("Hex", hex.EncodeToString(item1))
	printItem("Bytes", fmt.Sprintf("%d", len(item1)))
	fmt.Println()

	// Item 2: Stream ID → tags
	fmt.Println("--- Item 2: Stream ID → Tags Mapping ---")
	fmt.Println()
	streamTagsCanonical := "{app=\"nginx\",env=\"prod\",host=\"web-01\"}"
	item2 := constructStreamIDToTagsItem(&tenantID, &streamID, streamTagsCanonical)
	printItem("Purpose", "Given stream ID, retrieve human-readable tags")
	printItem("Format", "[0x01][tenantID 8B][streamID 16B][streamTagsCanonical]")
	printItem("Key Hex", hex.EncodeToString(item2[:25]))
	printItem("Value", streamTagsCanonical)
	printItem("Total Bytes", fmt.Sprintf("%d", len(item2)))
	fmt.Println()

	// Items 3-5: Tag → stream IDs
	fmt.Println("--- Items 3-5: Tag → Stream IDs (Inverted Index) ---")
	fmt.Println()

	for i, tag := range tags {
		fmt.Printf("--- Item %d: %s=\"%s\" ---\n", i+3, tag.Name, tag.Value)
		fmt.Println()
		item := constructTagToStreamIDsItem(&tenantID, &tag, &streamID)
		printItem("Purpose", fmt.Sprintf("Tag %s=\"%s\" → streamID", tag.Name, tag.Value))
		printItem("Format", "[0x02][tenantID 8B][tagName][separator][tagValue][streamID 16B]")
		printItem("Hex", hex.EncodeToString(item))
		printItem("Bytes", fmt.Sprintf("%d", len(item)))
		fmt.Println()
	}

	// Summary
	fmt.Println("=== Complete Registration Summary ===")
	fmt.Println()
	fmt.Printf("Stream: %s\n", formatTags(tags))
	fmt.Printf("StreamID: %s\n\n", &streamID)
	fmt.Println("Items emitted (5 total):")
	fmt.Println("  1. [0x00][tenantID][streamID]")
	fmt.Println("     Purpose: Stream existence marker")
	fmt.Println()
	fmt.Println("  2. [0x01][tenantID][streamID]{...}")
	fmt.Println("     Purpose: Stream ID → tags lookup")
	fmt.Println()
	for i, tag := range tags {
		fmt.Printf("  %d. [0x02][tenantID][%s][sep][%s][streamID]\n", i+3, tag.Name, tag.Value)
		fmt.Printf("     Purpose: Tag %s=\"%s\" → streamID\n", tag.Name, tag.Value)
		fmt.Println()
	}

	// Access patterns
	ShowAccessPatterns()
}

func constructStreamExistenceItem(tenantID *TenantID, streamID *StreamID) []byte {
	var buf []byte
	buf = append(buf, nsPrefixStreamID)
	buf = tenantID.Marshal(buf)
	buf = streamID.Marshal(buf)
	return buf
}

func constructStreamIDToTagsItem(tenantID *TenantID, streamID *StreamID, tags string) []byte {
	var buf []byte
	buf = append(buf, nsPrefixStreamIDToStreamTags)
	buf = tenantID.Marshal(buf)
	buf = streamID.Marshal(buf)
	buf = append(buf, tags...)
	return buf
}

func constructTagToStreamIDsItem(tenantID *TenantID, tag *StreamTag, streamID *StreamID) []byte {
	var buf []byte
	buf = append(buf, nsPrefixTagToStreamIDs)
	buf = tenantID.Marshal(buf)
	buf = tag.Marshal(buf)
	buf = streamID.Marshal(buf)
	return buf
}

func formatTags(tags []StreamTag) string {
	var parts []string
	for _, t := range tags {
		parts = append(parts, fmt.Sprintf("%s=\"%s\"", t.Name, t.Value))
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func printItem(label, value string) {
	fmt.Printf("  %-12s: %s\n", label, value)
}

// ==================== Access Patterns ====================

func ShowAccessPatterns() {
	fmt.Println("=== Access Patterns ===")
	fmt.Println()

	fmt.Println("--- Stream Existence Check ---")
	fmt.Println()
	fmt.Println("Query: 'Does stream X exist?'")
	fmt.Println()
	fmt.Println("Seek: [0x00][tenantID][streamID]")
	fmt.Println("If found    → stream exists")
	fmt.Println("If not found → stream doesn't exist")
	fmt.Println()

	fmt.Println("--- Stream Tags Lookup ---")
	fmt.Println()
	fmt.Println("Query: 'What are the tags for stream X?'")
	fmt.Println()
	fmt.Println("Seek: [0x01][tenantID][streamID]")
	fmt.Println("Value: {app=\"nginx\",env=\"prod\",host=\"web-01\"}")
	fmt.Println()

	fmt.Println("--- Tag Filter Resolution ---")
	fmt.Println()
	fmt.Println("Query: 'Which streams have app=\"nginx\"?'")
	fmt.Println()
	fmt.Println("Seek: [0x02][tenantID][app][sep][nginx]")
	fmt.Println("Scan forward while prefix matches")
	fmt.Println("Collect stream IDs from suffixes")
	fmt.Println()
	fmt.Println("Result: [S1, S5, S42, ...]")
	fmt.Println()
}
