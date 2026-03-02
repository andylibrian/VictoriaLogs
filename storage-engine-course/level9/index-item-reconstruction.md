# Level 9 Index Item Reconstruction

## Context
- Level: 9 - IndexDB and `mergeset` Deep Dive
- Date: 2026-03-02

## Example: Stream Registration

### Input

```
TenantID:   {AccountID: 12345, ProjectID: 0}
StreamID:   {hi: 0x0000000000000001, lo: 0x00000000000000AB}
StreamTags: {app="nginx", host="web-01", env="prod"}
```

### Namespace Prefixes

```go
const (
    nsPrefixStreamID            = 0  // Stream existence
    nsPrefixStreamIDToStreamTags = 1  // Stream ID → tags
    nsPrefixTagToStreamIDs       = 2  // Tag → stream IDs (inverted index)
)
```

### Item 1: Stream Existence Marker

**Purpose:** "Does this stream exist?" - Fast existence check

```
Format: [0x00][tenantID 8B][streamID 16B]

Key construction:
  [0x00]                           // nsPrefixStreamID = 0
  [0x00 0x00 0x00 0x00 0x00 0x00 0x30 0x39]  // tenantID = 12345
  [0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x01]  // streamID.hi
  [0x00 0x00 0x00 0x00 0x00 0x00 0x00 0xAB]  // streamID.lo

Value: (empty - key presence indicates existence)

Total: 25 bytes
```

### Item 2: Stream ID → Tags Mapping

**Purpose:** Given stream ID, retrieve human-readable tags

```
Format: [0x01][tenantID 8B][streamID 16B][streamTagsCanonical]

Key construction:
  [0x01]                           // nsPrefixStreamIDToStreamTags = 1
  [0x00 0x00 0x00 0x00 0x00 0x00 0x30 0x39]  // tenantID = 12345
  [0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x01]  // streamID.hi
  [0x00 0x00 0x00 0x00 0x00 0x00 0x00 0xAB]  // streamID.lo

Value construction:
  {app="nginx",env="prod",host="web-01"}  // Canonical sorted format

Total: 25 bytes (key) + N bytes (value)
```

### Item 3-5: Tag → Stream IDs (Inverted Index)

**Purpose:** Given tag filter, find all matching stream IDs

```
Format: [0x02][tenantID 8B][tagName][separator][tagValue][streamID 16B]

Each tag produces ONE inverted index item:
```

#### Item 3: app="nginx"

```
Key construction:
  [0x02]                           // nsPrefixTagToStreamIDs = 2
  [0x00 0x00 0x00 0x00 0x00 0x00 0x30 0x39]  // tenantID = 12345
  [0x03] [app]                     // tagName length + name
  [0xFF]                           // tagSeparatorChar = 0xFF
  [0x05] [nginx]                   // tagValue length + value
  [0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x01]  // streamID.hi
  [0x00 0x00 0x00 0x00 0x00 0x00 0x00 0xAB]  // streamID.lo

Total: 33 bytes (with 1 streamID)
```

#### Item 4: host="web-01"

```
Key construction:
  [0x02]                           // nsPrefixTagToStreamIDs = 2
  [0x00 0x00 0x00 0x00 0x00 0x00 0x30 0x39]  // tenantID = 12345
  [0x04] [host]                    // tagName length + name
  [0xFF]                           // tagSeparatorChar = 0xFF
  [0x06] [web-01]                  // tagValue length + value
  [0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x01]  // streamID.hi
  [0x00 0x00 0x00 0x00 0x00 0x00 0x00 0xAB]  // streamID.lo

Total: 35 bytes (with 1 streamID)
```

#### Item 5: env="prod"

```
Key construction:
  [0x02]                           // nsPrefixTagToStreamIDs = 2
  [0x00 0x00 0x00 0x00 0x00 0x00 0x30 0x39]  // tenantID = 12345
  [0x03] [env]                     // tagName length + name
  [0xFF]                           // tagSeparatorChar = 0xFF
  [0x04] [prod]                    // tagValue length + value
  [0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x01]  // streamID.hi
  [0x00 0x00 0x00 0x00 0x00 0x00 0x00 0xAB]  // streamID.lo

Total: 33 bytes (with 1 streamID)
```

## Complete Registration Summary

```
Stream: {app="nginx", host="web-01", env="prod"}
StreamID: 0x0000000000000001_00000000000000AB

Items emitted (5 total):

1. [0x00][tenantID][streamID]
   Purpose: Stream existence marker

2. [0x01][tenantID][streamID]{app="nginx",env="prod",host="web-01"}
   Purpose: Stream ID → tags lookup

3. [0x02][tenantID][app][sep][nginx][streamID]
   Purpose: Tag app="nginx" → streamID

4. [0x02][tenantID][host][sep][web-01][streamID]
   Purpose: Tag host="web-01" → streamID

5. [0x02][tenantID][env][sep][prod][streamID]
   Purpose: Tag env="prod" → streamID
```

## Access Patterns

### Stream Existence Check

```
Query: "Does stream X exist?"

Seek: [0x00][tenantID][streamID]
If found → stream exists
If not found → stream doesn't exist
```

### Stream Tags Lookup

```
Query: "What are the tags for stream X?"

Seek: [0x01][tenantID][streamID]
Value: {app="nginx",env="prod",host="web-01"}
```

### Tag Filter Resolution

```
Query: "Which streams have app='nginx'?"

Seek: [0x02][tenantID][app][sep][nginx]
Scan forward while prefix matches
Collect stream IDs from suffixes

Result: [S1, S5, S42, ...]
```

## Code References

- `lib/logstorage/indexdb.go:693-734` - `mustRegisterStream`
- `lib/logstorage/indexdb.go:86-101` - namespace prefix constants
- `lib/logstorage/indexdb.go:1176-1180` - `marshalCommonPrefix`

## Conclusions

1. **Three namespace types** - Existence, ID→tags, tag→IDs
2. **N+2 items per stream** - 1 existence + 1 ID→tags + N tag entries
3. **Prefix seeks** - All queries use prefix matching for efficiency
4. **Canonical sorting** - Tags sorted alphabetically for consistent keys

## Open Questions

- How does the separator character (0xFF) avoid conflicts with tag values?
- What happens if a tag name or value contains the separator character?
