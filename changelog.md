# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Added
### Changed
### Removed
### Fixed

## [0.0.1] - 2025-01-23
### Added
- wal file
- ConcurrentSkipList based memtable
- threshold sync to sstable
- put, get, del supported operation
- netty for tcp connection
- basic lexer for request tokenize
- basic parser for syntax checking
- interface wherever possible
- synchronous operation, blocking io -- for now

## [0.0.2] - 2025-01-23
### Added
- file compaction on threshold of two files
- UUID for naming the sstable file

## [0.0.3] - 2025-01-25
### Changed
- limited KeyValueSTore methods to put, get del
- LSMTree now governs compaction, flushMemtable
- SSTable interface and SSTableImpl representing an sstable file having reader and writer methods
### Added
- Using KeyValuePair to communicate across application
- Enumify the commands SET, GET, DEL for parser