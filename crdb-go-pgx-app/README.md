# Learning to Build a Go App with CockroachDB + Go px Driver
Following guide here: https://www.cockroachlabs.com/docs/v21.1/build-a-go-app-with-cockroachdb?filters=local

# Set Up
1. Ensure CockroachDB is installed
2. Start up a single node cluster:
```
cockroach start-single-node --advertise-addr 'localhost' --insecure
```
   This will create a directory called `cockroach-data` in the current directory.
3. Run `main.go`:
```
go run main.go
```
   The app will output a list of ids and their balances.
