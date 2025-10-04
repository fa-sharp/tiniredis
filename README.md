# tinykeyval

This is a Rust implementation of the CodeCrafters
["Build Your Own Redis" Challenge](https://codecrafters.io/challenges/redis).

Includes a Redis server that supports many of the basic commands for strings, lists, sets, sorted sets, geo hashes, and streams. Also includes a zero-copy implementation of the RESP2 protocol, and a basic Rust client that can send raw commands and pipelines to the Redis server.
