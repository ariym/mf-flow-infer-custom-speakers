package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"github.com/ariym/mf-flow-infer-custom-speakers/engine"
	"io"
	"os"
	"os/signal"
	"syscall"
)

//go:embed execution.json
var execution []byte

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var spec engine.Spec
	if err := json.Unmarshal(execution, &spec); err != nil {
		panic(err)
	}
	if len(os.Args) > 1 && os.Args[1] == "describe" {
		fmt.Println(string(execution))
		return
	}
	if len(os.Args) > 2 || (len(os.Args) == 2 && os.Args[1] != "preflight") {
		fmt.Fprintln(os.Stderr, "usage: flow [describe|preflight]")
		os.Exit(1)
	}
	done := make(chan []byte, 1)
	go func() { b, _ := io.ReadAll(io.LimitReader(os.Stdin, (1<<20)+1)); done <- b }()
	var b []byte
	select {
	case b = <-done:
	case <-ctx.Done():
		os.Exit(130)
	}
	var req engine.Request
	err := engine.Decode(b, &req)
	if len(b) > 1<<20 {
		err = fmt.Errorf("request exceeds 1 MiB")
	}
	var result map[string]any
	if err == nil {
		result, err = engine.Execute(ctx, req, spec, len(os.Args) == 2)
	}
	response := map[string]any{"protocol_version": 1}
	if err != nil {
		response["error"] = err.Error()
		fmt.Fprintln(os.Stderr, err)
	} else {
		response["result"] = result
	}
	json.NewEncoder(os.Stdout).Encode(response)
	if err != nil {
		os.Exit(1)
	}
}
