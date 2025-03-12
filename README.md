# Raft

This is a Go implementation of the Raft consensus algorithm, based on the MIT 6.5840 (2025) Lab 3. Raft is a distributed consensus algorithm designed to manage a replicated log in a fault-tolerant manner.

## Features

* Leader election
* Log replication
* Persistent state
* Snapshot
* Safety and consistency guarantees
* Fault tolerance with majority-based agreement

## Getting Started

### Installation

Clone this repository:

```
git clone https://github.com/zhux2/Raft.git
```

### Running the Tests

This project includes tests based on the MIT 6.5840 framework. You can run them with:

```
go test -v
```
or
```
# run a specific set of tests
go test -run 3[A|B|C|D]
```

## References

[Raft Paper (Ongaro & Ousterhout, 2014)](https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf)

[MIT 6.5840](https://pdos.csail.mit.edu/6.824/)

