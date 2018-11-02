package db

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
)

var MySqlDB *sql.DB

func InitMySQLDB(url string) *sql.DB {

	var err error
	MySqlDB, err = sql.Open("mysql", url)
	if err != nil {
		panic(err)
	}
	MySqlDB.SetMaxOpenConns(50)
	MySqlDB.SetMaxIdleConns(25)
	err = MySqlDB.Ping()
	if err != nil {
		panic(err)
	}
	return MySqlDB
}

func ExecuteUpdate(sql string, args ...interface{}) error {

	fmt.Println(args)

	stm, err := MySqlDB.Prepare(sql)
	defer stm.Close()

	if err != nil {
		fmt.Println(err)
		return err
	}

	_, err = stm.Exec(args...)

	if err != nil {
		fmt.Println(err)

		return err
	}

	return nil
}
