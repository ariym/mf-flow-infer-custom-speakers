package engine

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
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

func TestStrictJSONAndPrecision(t *testing.T) {
	for _, text := range []string{`{"x":1,"x":2}`, `{"a":[{"x":1,"x":2}]}`, `{"x":{}} {}`} {
		var value map[string]any
		if Decode([]byte(text), &value) == nil {
			t.Fatalf("accepted ambiguous JSON: %s", text)
		}
	}
	var value map[string]any
	if err := Decode([]byte(`{"large":9007199254740993}`), &value); err != nil {
		t.Fatal(err)
	}
	if value["large"] != json.Number("9007199254740993") {
		t.Fatal("lost integer precision")
	}
}

func TestRunFailureAndIncompleteDestination(t *testing.T) {
	req, spec, log := fixture(t)
	script, _ := os.ReadFile(req.Programs["demo"].Path)
	os.WriteFile(req.Programs["demo"].Path, []byte(strings.Replace(string(script), `'{"result":{"voice":{"voice_id":"a"}}}'`, `'invalid result'`, 1)), 0700)
	if _, err := Execute(context.Background(), req, spec, false); err == nil {
		t.Fatal("accepted broken response")
	}
	stages, _ := filepath.Glob(filepath.Join(filepath.Dir(log), ".mf-"+req.JobID+"-*"))
	if len(stages) != 0 {
		t.Fatal("left incomplete staging files")
	}
	os.WriteFile(req.Programs["demo"].Path, script, 0700)
	os.WriteFile(log, nil, 0600)
	os.Mkdir(filepath.Join(filepath.Dir(log), req.JobID), 0700)
	if _, err := Execute(context.Background(), req, spec, false); err == nil {
		t.Fatal("accepted incomplete destination")
	}
	calls, _ := os.ReadFile(log)
	if strings.Contains(string(calls), "run") {
		t.Fatal("inference started with occupied output destination")
	}
}

func TestCancelRunningSubprocessGroup(t *testing.T) {
	req, spec, log := fixture(t)
	spec.Steps = spec.Steps[:1]
	script := `#!/bin/sh
cat >/dev/null
case "$1" in
 describe) echo '{"program_name":"demo","version":1}';;
 preflight) echo '{"ready":true}';;
 run) echo started > '` + log + `'; trap '' TERM; sleep 30 & wait;;
esac
`
	os.WriteFile(req.Programs["demo"].Path, []byte(script), 0700)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := Execute(ctx, req, spec, false); done <- err }()
	deadline := time.After(10 * time.Second)
	for {
		if content, _ := os.ReadFile(log); strings.Contains(string(content), "started") {
			break
		}
		select {
		case <-deadline:
			t.Fatal("subprocess never started")
		case <-time.After(10 * time.Millisecond):
		}
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("cancellation succeeded unexpectedly")
		}
	case <-time.After(8 * time.Second):
		t.Fatal("subprocess group did not terminate")
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(log), req.JobID)); !os.IsNotExist(err) {
		t.Fatal("canceled job committed")
	}
	stages, _ := filepath.Glob(filepath.Join(filepath.Dir(log), ".mf-"+req.JobID+"-*"))
	if len(stages) != 0 {
		t.Fatal("canceled job left staging files")
	}
}
