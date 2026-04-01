package main

import (
	"bedrockdb/internal/db"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/chzyer/readline"
)

func main() {
	database, err := db.Open("./data")
	if err != nil {
		log.Fatal(err)
	}
	defer database.Close()
	homeDir, err := os.UserHomeDir()
	if err != nil {
		log.Fatal(err)
	}

	historyPath := filepath.Join(homeDir, ".bedrock_history")
	fmt.Println("History file:", historyPath)
	completer := readline.NewPrefixCompleter(
		readline.PcItem("put"),
		readline.PcItem("get"),
		readline.PcItem("delete"),
		readline.PcItem("range"),
		readline.PcItem("exit"),
		readline.PcItem("stats"),
		readline.PcItem("lsm"),
		readline.PcItem("sstables"),
	)

	rl, err := readline.NewEx(&readline.Config{
		Prompt:          "bedrockdb> ",
		HistoryFile:     historyPath,
		AutoComplete:    completer,
		InterruptPrompt: "^C",
		EOFPrompt:       "exit",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer rl.Close()

	fmt.Println("BedrockDB CLI (history + autocomplete enabled)")

	for {
		line, err := rl.Readline()
		if err == readline.ErrInterrupt {
			continue // Ctrl+C → don't exit
		} else if err != nil {
			break // Ctrl+D
		}

		input := strings.TrimSpace(line)

		if input == "" {
			continue
		}
		if input == "exit" {
			break
		}

		handleCommand(database, input)
	}
}

func handleCommand(database *db.DB, input string) {
	parts := strings.Fields(input)

	switch parts[0] {

	case "put":
		if len(parts) < 3 {
			fmt.Println("usage: put <key> <value>")
			return
		}
		key := parts[1]
		value := strings.Join(parts[2:], " ")

		err := database.Put(key, value)
		if err != nil {
			fmt.Println("error:", err)
			return
		}
		fmt.Println("OK")

	case "get":
		if len(parts) < 2 {
			fmt.Println("usage: get <key>")
			return
		}
		key := parts[1]

		val, found, err := database.Get(key)
		if err != nil {
			fmt.Println("error:", err)
			return
		}
		if !found {
			fmt.Println("(nil)")
			return
		}
		fmt.Println(val)

	case "delete":
		if len(parts) < 2 {
			fmt.Println("usage: delete <key>")
			return
		}
		key := parts[1]

		err := database.Delete(key)
		if err != nil {
			fmt.Println("error:", err)
			return
		}
		fmt.Println("OK")

	case "range":
		if len(parts) < 3 {
			fmt.Println("usage: range <start> <end>")
			return
		}
		start := parts[1]
		end := parts[2]

		results, err := database.Range(start, end)
		if err != nil {
			fmt.Println("error:", err)
			return
		}

		for _, kv := range results {
			fmt.Printf("%s = %s\n", kv.Key, kv.Value)
		}
	case "stats":
		fmt.Println(database.Stats())
	case "sstables":
		fmt.Print(database.SSTables())

	default:
		fmt.Println("unknown command")
	}
}
