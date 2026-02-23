// Package logstorage defines the file and directory naming conventions used throughout VictoriaLogs.
//
// This file centralizes all file/directory name constants. Understanding these names
// is essential for navigating the codebase and debugging on-disk data.
//
// ON-DISK STRUCTURE OVERVIEW:
//
//	<storageDataPath>/
//	├── delete_tasks.json         # Persistent delete task definitions
//	└── partitions/
//	    └── 20260101/             # Partition directory (YYYYMMDD format)
//	        ├── indexdb/           # Stream metadata storage
//	        │   └── <mergeset files>
//	        ├── datadb/            # Log data storage
//	        │   ├── parts.json     # List of part directories
//	        │   └── <part_dirs>/   # Each part contains block files
//	        └── snapshots/         # Created on demand
//	            └── 20260101120000-00000001/
//	                ├── indexdb/   # Hard-linked index snapshot
//	                └── datadb/    # Hard-linked data snapshot
//
// FILE PURPOSES:
//
// BLOCK DATA FILES (inside each part directory):
//   - column_names.bin: Column/field names in log blocks
//   - column_idxs.bin: Column indexes mapping names to positions
//   - metaindex.bin: Block metadata for fast block location
//   - index.bin: Index for locating block data within files
//   - timestamps.bin: Compressed log timestamps
//   - columns_header.bin: Column header metadata
//   - columns_header_index.bin: Index for column headers
//   - values.bin: Compressed field values (current format)
//   - bloom.bin: Bloom filter for quick field existence checks
//   - message_values.bin: Message field values (optimized for _message queries)
//   - message_bloom.bin: Bloom filter for message field
//   - field_values.bin: Legacy field values (pre-zstd refactoring)
//   - field_bloom.bin: Legacy bloom filter (pre-zstd refactoring)
//
// METADATA FILES:
//   - metadata.json: Part header containing rows count, timestamps range, sizes
//   - parts.json: JSON array listing all part directory names in this partition
//   - delete_tasks.json: Persistent storage for delete task definitions
//
// DIRECTORY NAMES:
//   - indexdb: Contains mergeset tables for stream metadata
//   - datadb: Contains log data organized into parts
//   - partitions: Parent directory for all per-day partitions
//   - snapshots: Contains point-in-time backup snapshots
package logstorage

const (
	columnNamesFilename        = "column_names.bin"
	columnIdxsFilename         = "column_idxs.bin"
	metaindexFilename          = "metaindex.bin"
	indexFilename              = "index.bin"
	columnsHeaderIndexFilename = "columns_header_index.bin"
	columnsHeaderFilename      = "columns_header.bin"
	timestampsFilename         = "timestamps.bin"
	oldValuesFilename          = "field_values.bin"
	oldBloomFilename           = "field_bloom.bin"
	valuesFilename             = "values.bin"
	bloomFilename              = "bloom.bin"
	messageValuesFilename      = "message_values.bin"
	messageBloomFilename       = "message_bloom.bin"

	metadataFilename = "metadata.json"
	partsFilename    = "parts.json"

	deleteTasksFilename = "delete_tasks.json"

	indexdbDirname    = "indexdb"
	datadbDirname     = "datadb"
	partitionsDirname = "partitions"
	snapshotsDirname  = "snapshots"
)
