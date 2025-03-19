# Changelog

## [0.0.7] - 2025-03-19
### Changed
- Changed bloom filter configuration, 5000000 bit array
- using 10 tier of sstables list
### Added
- readme

## [0.0.6] - 2025-03-14
### Changed
- fixed bug, few keys were returning null values
- renamed the SSTable and BLoomFilter implementation classes and removed there interfaces
- adding sstable to the first index in sstable instead of last,
- removed redundant memtable status
- throwing RuntimeException incase there is any exception
### Added
- get and add benchmark

## [0.0.5] - 2025-02-14
### Changed
- size tiered compaction
- added getSize method in SSTable
- coarse grained locking to mitigate race conditions
### Added
- tree get and put test case

## [0.0.4] - 2025-02-13
### Changed
- File structure change, each sstable will have there separate folder with bloom, sparse file
- Project structure changed
- DB recovery on restart, loading already persisted files
- running on port 6371 to avoid clashes with redis server
### Added
- Thread safe implementation with synchronized
- Complete Bloom filter implementation
- Google hashing libraries
- Complete SSTable implementation
- Several bug fixes in LSM implementation in general
- Started implementing Benchmarking and testing utility

## [0.0.3] - 2025-01-25
### Changed
- limited KeyValueSTore methods to put, get del
- LSMTree now governs compaction, flushMemtable
- SSTable interface and SSTableImpl representing an sstable file having reader and writer methods
### Added
- Using KeyValuePair to communicate across application
- Enumify the commands SET, GET, DEL for parser

## [0.0.2] - 2025-01-23
### Added
- file compaction on threshold of two files
- UUID for naming the sstable file

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

---

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Added
### Changed
### Removed
### Fixed