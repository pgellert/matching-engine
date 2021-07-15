# High-availability matching engine of a stock exchange

This is my dissertation project for the completion of my BA degree in Computer Science Tripos 2020-2021 at the 
University of Cambridge.

## Summary

This is the implementation of a highly-available matching engine with a simplified interface. The engine handles buy, 
sell and cancel orders, and outstanding orders are stored in a replicated order book. It acknowledges to clients when 
their orders have been placed, and notifies them of trades occurring through the order book. The replicated matching 
engine tolerates machine and network failures, including partitions in the network. My implementation of the Raft 
consensus protocol is used to replicate the matching engine using state-machine replication.

## Repository overview

The code for the matching engine is divided into two modules: engine and raft.

The raft component contains code for state-machine replication using the Raft protocol, while the engine component 
depends on the raft module to build a replicated matching engine and a client port.

Chapter 2 of my dissertation has detailed explanation on how the implementation works and how the code is structured.


## Performance

 - Failover latency: 150ms
 - Matching latency: 40-200ns
 - End-to-end latency: 200ms
 - End-to-end throughput: 750RPS (single-client)

Chapter 3 of my dissertation has more context on these results and how they were measured.


## Useful links:

 - [Raft paper](https://web.stanford.edu/~ouster/cgi-bin/papers/raft-atc14.pdf)
 - [My dissertation](dissertation.pdf)

