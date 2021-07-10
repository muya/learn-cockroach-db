package main

import (
	"context"
	"fmt"
	"log"

	"github.com/cockroachdb/cockroach-go/v2/crdb/crdbpgx"
	"github.com/jackc/pgx/v4"
)

func main() {
	config, parseConfigErr := pgx.ParseConfig("postgres://crdb_user:crdb_pass@127.0.0.1:26257/bank?sslmode=require")
	if parseConfigErr != nil {
		log.Fatal("error configuring the database: ", parseConfigErr)
	}

	// Connect to the database
	conn, connectErr := pgx.ConnectConfig(context.Background(), config)
	if connectErr != nil {
		log.Fatal("error connecting to the db: ", connectErr)
	}
	defer conn.Close(context.Background())

	// Re-create accounts table
	createAccTblStmt := "DROP TABLE IF EXISTS accounts; CREATE TABLE IF NOT EXISTS accounts (id INT PRIMARY KEY, balance INT);"
	if _, err := conn.Exec(context.Background(), createAccTblStmt); err != nil {
		log.Fatal("error creating accounts table: ", err)
	}

	// insert 2 rows into accounts table
	insertRowStmt := "INSERT INTO accounts (id, balance) VALUES (1, 1000), (2, 250)"
	if _, err := conn.Exec(context.Background(), insertRowStmt); err != nil {
		log.Fatal("error inserting accounts: ", err)
	}

	// print out the balances
	queryAccStmt := "SELECT id, balance FROM accounts"
	rows, fetchRowsErr := conn.Query(context.Background(), queryAccStmt)
	if fetchRowsErr != nil {
		log.Fatal("error querying accounts: ", fetchRowsErr)
	}
	defer rows.Close()

	fmt.Println("Initial balances:")

	for rows.Next() {
		var id, balance int

		if err := rows.Scan(&id, &balance); err != nil {
			log.Fatal("Error parsing row: ", err)
		}

		fmt.Printf("%d %d\n", id, balance)
	}

	// Run a transfer within a transaction
	transferFn := func(tx pgx.Tx) error {
		fromAccountId := 1
		toAccountId := 2
		transferAmount := 100

		return transferFunds(context.Background(), tx, fromAccountId, toAccountId, transferAmount)
	}
	if err := crdbpgx.ExecuteTx(context.Background(), conn, pgx.TxOptions{}, transferFn); err != nil {
		log.Fatal("Error while transferring funds - ", err)
	}

	fmt.Println("Transfer successful!")
}

func transferFunds(ctx context.Context, tx pgx.Tx, from int, to int, amount int) error {
	queryBalanceStatement := "SELECT balance FROM accounts WHERE id = $1"

	// First read the balance
	var fromBalance int
	if err := tx.QueryRow(ctx, queryBalanceStatement, from).Scan(&fromBalance); err != nil {
		return err
	}

	if fromBalance < amount {
		return fmt.Errorf("insufficient funds in account ID %d", from)
	}

	deductBalanceQuery := "UPDATE accounts SET balance = (balance - $1) WHERE id = $2 LIMIT 1;"
	addBalanceQuery := "UPDATE accounts SET balance = (balance + $1) WHERE id = $2 LIMIT 1;"

	if _, err := tx.Exec(ctx, deductBalanceQuery, amount, from); err != nil {
		return fmt.Errorf("error deducting amount from account ID %d - %w", from, err)
	}

	if _, err := tx.Exec(ctx, addBalanceQuery, amount, to); err != nil {
		return fmt.Errorf("error adding amount to account ID %d - %w", to, err)
	}

	return nil
}
