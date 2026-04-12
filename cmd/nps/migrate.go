package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"ehang.io/nps/lib/common"
	"ehang.io/nps/lib/file"

	"github.com/astaxie/beego/logs"
)

func migrateData() {
	migrateCmd := flag.NewFlagSet("migrate", flag.ExitOnError)
	dsn := migrateCmd.String("mysql_dsn", "", "MySQL DSN, format: username:password@tcp(host:port)/dbname?charset=utf8mb4&parseTime=true&loc=Local")
	confPath := migrateCmd.String("conf_path", "", "NPS config path (where conf/ directory is located)")

	migrateCmd.Parse(os.Args[2:])

	if *dsn == "" {
		fmt.Println("Error: mysql_dsn is required")
		fmt.Println("Usage: nps migrate -mysql_dsn=root:password@tcp(127.0.0.1:3306)/nps?charset=utf8mb4&parseTime=true&loc=Local [-conf_path=/etc/nps]")
		os.Exit(1)
	}

	runPath := *confPath
	if runPath == "" {
		runPath = common.GetRunPath()
	}

	configFile := filepath.Join(runPath, "conf", "nps.conf")
	if _, err := os.Stat(configFile); err != nil {
		fmt.Printf("Config file not found: %s\n", configFile)
		fmt.Println("Please specify -conf_path to your NPS installation directory")
		os.Exit(1)
	}

	_ = logs.SetLogger(logs.AdapterConsole, `{"level":7,"color":true}`)

	fmt.Printf("Migrating data from JSON files to MySQL...\n")
	fmt.Printf("Config path: %s\n", runPath)
	fmt.Printf("MySQL DSN: %s\n", maskDsn(*dsn))

	if err := file.MigrateFromJson(*dsn, runPath); err != nil {
		fmt.Printf("Migration failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Migration completed successfully!")
	fmt.Println("")
	fmt.Println("Next steps:")
	fmt.Println("1. Verify data in MySQL")
	fmt.Println("2. Add mysql_dsn to nps.conf:")
	fmt.Printf("   mysql_dsn=%s\n", *dsn)
	fmt.Println("3. Restart NPS service")
}

func maskDsn(dsn string) string {
	atIdx := strings.Index(dsn, "@")
	if atIdx == -1 {
		return dsn
	}
	prefix := dsn[:atIdx]
	colonIdx := strings.Index(prefix, ":")
	if colonIdx == -1 {
		return dsn
	}
	return prefix[:colonIdx] + ":****" + dsn[atIdx:]
}
