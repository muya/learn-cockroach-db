package main

import (
	"bytes"
	"context"
	"fmt"
	"github.com/cockroachdb/cockroach-go/v2/crdb/crdbpgx"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"
	"log"
)

type Account struct {
	ID      uuid.UUID
	balance int
}

func BuildInsertAccountStatement(accounts []Account) string {
	baseSql := "INSERT INTO accounts (id, balance) VALUES "

	buf := bytes.Buffer{}

	buf.WriteString(baseSql)

	for i := 0; i < len(accounts); i++ {
		param1 := (i * 2) + 1
		param2 := (i * 2) + 2

		currStr := fmt.Sprintf("($%d, $%d)", param1, param2)

		buf.WriteString(currStr)

		if (i + 1) < len(accounts) {
			// This is not the last item; add a comma and space
			buf.WriteString(", ")
		}
	}

	return buf.String()
}

func BuildInsertAccountParams(accounts []Account) []interface{} {
	// We're creating a fixed size array. As such, we shouldn't use append, since that will just add to the end of array
	accountParams := make([]interface{}, len(accounts)*2)

	for i, account := range accounts {
		accountParams[i*2] = account.ID
		accountParams[(i*2)+1] = account.balance
	}

	println(fmt.Sprintf("params: %s", accountParams))

	return accountParams
}

func insertAccounts(ctx context.Context, tx pgx.Tx, accounts []Account) error {
	// Build insert statement
	if len(accounts) < 1 {
		return fmt.Errorf("at least 1 account needed for insertion")
	}

	insertAccountStmts := BuildInsertAccountStatement(accounts)
	insertAccountParams := BuildInsertAccountParams(accounts)

	if _, err := tx.Exec(ctx, insertAccountStmts, insertAccountParams...); err != nil {
		return errors.Wrap(err, "error inserting accounts")
	}

	return nil
}

func main() {
	// Read in connection string
	config, parseConfigErr := pgx.ParseConfig("postgres://root@127.0.0.1:26257?sslmode=disable")
	if parseConfigErr != nil {
		log.Fatal("error configuring the database: ", parseConfigErr)
	}

	config.Database = "bank"

	// Connect to the database
	conn, connectErr := pgx.ConnectConfig(context.Background(), config)
	if connectErr != nil {
		log.Fatal("error connecting to the db: ", connectErr)
	}
	defer conn.Close(context.Background())

	// Re-create accounts table
	// createAccTblStmt := "DROP TABLE IF EXISTS accounts; CREATE TABLE IF NOT EXISTS accounts (id INT PRIMARY KEY, balance INT);"
	// if _, err := conn.Exec(context.Background(), createAccTblStmt); err != nil {
	// 	log.Fatal("error creating accounts table: ", err)
	// }

	/*
		CockroachDB may require the client to retry a transaction in the case of read/write contention.
		The CockroachDB Go client includes a generic retry function (ExecuteTx()) that runs inside a transaction and
		retries it as needed. The code sample shows how you can use this function to wrap SQL statements.
	*/
	// Insert initial rows
	accounts := []Account{
		{ID: uuid.New(), balance: 250},
		{ID: uuid.New(), balance: 100},
		{ID: uuid.New(), balance: 500},
		{ID: uuid.New(), balance: 300},
	}

	insertErr := crdbpgx.ExecuteTx(context.Background(), conn, pgx.TxOptions{}, func(tx pgx.Tx) error {
		return insertAccounts(context.Background(), tx, accounts)
	})

	if insertErr != nil {
		log.Fatal("error inserting accounts: ", insertErr)
	}

	// // insert 2 rows into accounts table
	// insertRowStmt := "INSERT INTO accounts (id, balance) VALUES (1, 1000), (2, 250)"
	// if _, err := conn.Exec(context.Background(), insertRowStmt); err != nil {
	// 	log.Fatal("error inserting accounts: ", err)
	// }

	// print out the balances
	queryAccStmt := "SELECT id, balance FROM accounts"
	rows, fetchRowsErr := conn.Query(context.Background(), queryAccStmt)
	if fetchRowsErr != nil {
		log.Fatal("error querying accounts: ", fetchRowsErr)
	}
	defer rows.Close()

	fmt.Println("Initial balances:")

	for rows.Next() {
		var id uuid.UUID
		var balance int

		if err := rows.Scan(&id, &balance); err != nil {
			log.Fatal("Error parsing row: ", err)
		}

		fmt.Printf("%s %d\n", id, balance)
	}

	// TODO: Continue with implementation of other methods

	// // Run a transfer within a transaction
	// transferFn := func(tx pgx.Tx) error {
	// 	fromAccountId := 1
	// 	toAccountId := 2
	// 	transferAmount := 100
	//
	// 	return transferFunds(context.Background(), tx, fromAccountId, toAccountId, transferAmount)
	// }
	// if err := crdbpgx.ExecuteTx(context.Background(), conn, pgx.TxOptions{}, transferFn); err != nil {
	// 	log.Fatal("Error while transferring funds - ", err)
	// }
	//
	// fmt.Println("Transfer successful!")
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
