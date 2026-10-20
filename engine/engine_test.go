package engine

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func fixture(t *testing.T) (Request, Spec, string) {
	t.Helper()
	root := t.TempDir()
	db := filepath.Join(root, "client.db")
	os.WriteFile(db, []byte("existing"), 0600)
	prog := filepath.Join(root, "program")
	log := filepath.Join(root, "calls")
	script := `#!/bin/sh
cat >/dev/null
if [ "$1" = describe ]; then printf '%s\n' '{"program_name":"demo","version":1}'; exit 0; fi
printf '%s\n' "$1" >> '` + log + `'
if [ "$1" = preflight ]; then printf '%s\n' '{"ready":true}'; else printf '%s\n' '{"result":{"voice":{"voice_id":"a"}}}'; fi
`
	os.WriteFile(prog, []byte(script), 0700)
	output, _ := json.Marshal(root)
	req := Request{ProtocolVersion: 1, JobID: "job-1", DBPath: db, Input: map[string]json.RawMessage{"output_dir": output}, Programs: map[string]Dependency{"demo": {Version: 1, Path: prog}}}
	spec := Spec{Key: "test", Programs: map[string]int{"demo": 1}, Steps: []Step{{Program: "demo", Operation: "one"}, {Program: "demo", Operation: "two"}}, Tags: map[string]int{"voice": 1}}
	return req, spec, log
}
func TestPreflightBarrierAndRetry(t *testing.T) {
	req, spec, log := fixture(t)
	result, err := Execute(context.Background(), req, spec, false)
	if err != nil {
		t.Fatal(err)
	}
	calls, _ := os.ReadFile(log)
	if string(calls) != "preflight\npreflight\nrun\nrun\n" {
		t.Fatalf("bad ordering: %s", calls)
	}
	if len(result["tags"].([]any)) != 2 {
		t.Fatal("tags missing")
	}
	if _, err = Execute(context.Background(), req, spec, false); err != nil {
		t.Fatal(err)
	}
	calls, _ = os.ReadFile(log)
	if strings.Count(string(calls), "run") != 2 {
		t.Fatal("retry repeated inference")
	}
	req.Input["options"] = json.RawMessage(`{"changed":true}`)
	if _, err = Execute(context.Background(), req, spec, false); err == nil {
		t.Fatal("accepted conflicting retry")
	}
}
func TestPreflightFailureNoCommit(t *testing.T) {
	req, spec, log := fixture(t)
	bad := filepath.Join(filepath.Dir(log), "bad")
	os.WriteFile(bad, []byte("#!/bin/sh\ncat >/dev/null\necho failure >&2\nexit 1\n"), 0700)
	req.Programs["bad"] = Dependency{1, bad}
	spec.Programs["bad"] = 1
	spec.Steps[1].Program = "bad"
	if _, err := Execute(context.Background(), req, spec, false); err == nil {
		t.Fatal("expected preflight error")
	}
	calls, _ := os.ReadFile(log)
	if strings.Contains(string(calls), "run") {
		t.Fatal("inference started before all preflight passed")
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(log), req.JobID)); !os.IsNotExist(err) {
		t.Fatal("partial result committed")
	}
}
func TestPreflightOnlyAndCanceled(t *testing.T) {
	req, spec, log := fixture(t)
	if _, err := Execute(context.Background(), req, spec, true); err != nil {
		t.Fatal(err)
	}
	calls, _ := os.ReadFile(log)
	if strings.Contains(string(calls), "run") {
		t.Fatal("preflight started inference")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Execute(ctx, req, spec, false); err == nil {
		t.Fatal("ignored cancellation")
	}
}
