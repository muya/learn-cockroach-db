package main

import (
	"github.com/google/uuid"
	"testing"
)

func TestBuildInsertAccountStatement(t *testing.T) {
	tests := []struct {
		name        string
		accounts    []Account
		expectedSql string
	}{
		{
			name: "single account",
			accounts: []Account{
				{ID: uuid.New(), balance: 50},
			},
			expectedSql: "INSERT INTO accounts (id, balance) VALUES ($1, $2)",
		},
		{
			name: "2 accounts",
			accounts: []Account{
				{ID: uuid.New(), balance: 50},
				{ID: uuid.New(), balance: 500},
			},
			expectedSql: "INSERT INTO accounts (id, balance) VALUES ($1, $2), ($3, $4)",
		},
		{
			name: "5 accounts",
			accounts: []Account{
				{ID: uuid.New(), balance: 50},
				{ID: uuid.New(), balance: 500},
				{ID: uuid.New(), balance: 700},
				{ID: uuid.New(), balance: 900},
				{ID: uuid.New(), balance: 1500},
			},
			expectedSql: "INSERT INTO accounts (id, balance) VALUES ($1, $2), ($3, $4), ($5, $6), ($7, $8), ($9, $10)",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			createdSql := BuildInsertAccountStatement(test.accounts)

			if createdSql != test.expectedSql {
				t.Errorf("Expected generated SQL to be [%s], found [%s]", test.expectedSql, createdSql)
			}
		})
	}
}
