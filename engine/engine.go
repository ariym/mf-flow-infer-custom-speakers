// Package engine implements file-oriented, atomic standalone flows. It has no
// dependency on a client database schema or on mf-hub.
package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"syscall"
	"time"
)

type Dependency struct {
	Version int    `json:"version"`
	Path    string `json:"path"`
}
type Request struct {
	ProtocolVersion int                        `json:"protocol_version"`
	JobID           string                     `json:"job_id"`
	DBPath          string                     `json:"db_path"`
	Input           map[string]json.RawMessage `json:"input"`
	Programs        map[string]Dependency      `json:"programs"`
	Flows           map[string]Dependency      `json:"flows"`
}
type Step struct {
	Program    string `json:"program"`
	Operation  string `json:"operation"`
	DataKey    string `json:"data_key,omitempty"`
	WhenOption string `json:"when_option,omitempty"`
}
type Spec struct {
	Key      string         `json:"key"`
	Steps    []Step         `json:"steps"`
	Programs map[string]int `json:"programs"`
	Tags     map[string]int `json:"tags"`
}

func Decode(data []byte, value any) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	d.DisallowUnknownFields()
	if err := d.Decode(value); err != nil {
		return err
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return errors.New("expected one JSON object")
	}
	return nil
}
func Hash(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err = io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
func Invoke(ctx context.Context, executable, command string, request any) (map[string]any, error) {
	body, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(ctx, executable, command)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM) }
	cmd.WaitDelay = 3 * time.Second
	cmd.Stdin = bytes.NewReader(body)
	cmd.Stderr = os.Stderr
	var out bytes.Buffer
	cmd.Stdout = &out
	err = cmd.Run()
	if ctx.Err() != nil && cmd.Process != nil {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	if err != nil {
		return nil, fmt.Errorf("%s %s: %w: %s", filepath.Base(executable), command, err, strings.TrimSpace(out.String()))
	}
	var result map[string]any
	if err = Decode(out.Bytes(), &result); err != nil {
		return nil, fmt.Errorf("invalid program response: %w", err)
	}
	if result["error"] != nil {
		return nil, fmt.Errorf("program error: %v", result["error"])
	}
	return result, nil
}
func object(raw json.RawMessage) (map[string]any, error) {
	if len(raw) == 0 {
		return map[string]any{}, nil
	}
	var m map[string]any
	err := Decode(raw, &m)
	if err == nil && m == nil {
		err = errors.New("expected an object")
	}
	return m, err
}
func enabled(step Step, req Request) bool {
	if step.WhenOption == "" {
		return true
	}
	options, _ := object(req.Input["options"])
	value, ok := options[step.WhenOption].(bool)
	return !ok || value
}
func programRequest(step Step, req Request) (map[string]any, error) {
	r := map[string]any{"operation": step.Operation}
	for _, key := range []string{"media", "profiles", "voice_id", "label", "threshold", "data"} {
		if raw, ok := req.Input[key]; ok {
			var value any
			if err := Decode(raw, &value); err != nil {
				return nil, err
			}
			r[key] = value
		}
	}
	options := map[string]any{}
	// Top-level options configure the final operation; per-program options configure
	// preliminary operations without leaking unrelated flags across backends.
	overrides, err := object(req.Input["program_options"])
	if err != nil {
		return nil, err
	}
	if step.DataKey == "" {
		options, err = object(req.Input["options"])
		if err != nil {
			return nil, err
		}
	}
	if v, ok := overrides[step.Program]; ok {
		m, ok := v.(map[string]any)
		if !ok {
			return nil, errors.New("program_options values must be objects")
		}
		for k, v := range m {
			options[k] = v
		}
	}
	r["options"] = options
	return r, nil
}

func Execute(ctx context.Context, req Request, spec Spec, onlyPreflight bool) (map[string]any, error) {
	if req.ProtocolVersion != 1 || !regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`).MatchString(req.JobID) {
		return nil, errors.New("protocol_version 1 and a safe stable job_id are required")
	}
	if !filepath.IsAbs(req.DBPath) {
		return nil, errors.New("db_path must be absolute")
	}
	if st, err := os.Stat(req.DBPath); err != nil || !st.Mode().IsRegular() {
		return nil, errors.New("db_path must identify an existing regular file")
	}
	if req.Input == nil {
		return nil, errors.New("input must be an object")
	}
	allowed := map[string]bool{}
	for _, k := range []string{"media", "profiles", "voice_id", "label", "threshold", "data", "options", "program_options", "output_dir"} {
		allowed[k] = true
	}
	for key := range req.Input {
		if !allowed[key] {
			return nil, fmt.Errorf("unknown input field: %s", key)
		}
	}
	var output string
	if err := json.Unmarshal(req.Input["output_dir"], &output); err != nil || !filepath.IsAbs(output) {
		return nil, errors.New("input.output_dir must be absolute")
	}
	root, err := filepath.EvalSymlinks(output)
	if err != nil {
		return nil, fmt.Errorf("output_dir must exist: %w", err)
	}
	if st, err := os.Stat(root); err != nil || !st.IsDir() {
		return nil, errors.New("output_dir must be a directory")
	}
	if syscall.Access(root, 2) != nil {
		return nil, errors.New("output_dir must be writable")
	}
	identities := map[string]any{}
	for name, version := range spec.Programs {
		d, ok := req.Programs[name]
		if !ok || d.Version != version || !filepath.IsAbs(d.Path) {
			return nil, fmt.Errorf("required program %s@%d is missing or incompatible", name, version)
		}
		st, err := os.Stat(d.Path)
		if err != nil || !st.Mode().IsRegular() || st.Mode().Perm()&0111 == 0 {
			return nil, fmt.Errorf("program %s is not executable", name)
		}
		description, err := Invoke(ctx, d.Path, "describe", nil)
		if err != nil {
			return nil, err
		}
		if description["program_name"] != name || fmt.Sprint(description["version"]) != fmt.Sprint(version) {
			return nil, fmt.Errorf("executable does not identify as %s@%d", name, version)
		}
		checksum, err := Hash(d.Path)
		if err != nil {
			return nil, err
		}
		identities[name] = map[string]any{"sha256": checksum, "description": description}
	}
	if options, err := object(req.Input["program_options"]); err != nil {
		return nil, err
	} else {
		for k := range options {
			if _, ok := spec.Programs[k]; !ok {
				return nil, fmt.Errorf("unknown program_options dependency: %s", k)
			}
		}
	}
	checks := []any{}
	requests := []map[string]any{}
	steps := []Step{}
	// Complete every prerequisite and decode before invoking the first run command.
	for _, step := range spec.Steps {
		if !enabled(step, req) {
			continue
		}
		r, err := programRequest(step, req)
		if err != nil {
			return nil, err
		}
		result, err := Invoke(ctx, req.Programs[step.Program].Path, "preflight", r)
		if err != nil {
			return nil, err
		}
		checks = append(checks, result)
		requests = append(requests, r)
		steps = append(steps, step)
	}
	if onlyPreflight {
		return map[string]any{"ready": true, "checks": checks}, nil
	}
	fingerprintInput := map[string]any{"flow": spec.Key, "input": req.Input, "checks": checks, "programs": req.Programs, "identities": identities}
	canonical, _ := json.Marshal(fingerprintInput)
	sum := sha256.Sum256(canonical)
	fingerprint := hex.EncodeToString(sum[:])
	final := filepath.Join(root, req.JobID)
	if old, err := os.ReadFile(filepath.Join(final, "result.json")); err == nil {
		var result map[string]any
		if err = Decode(old, &result); err != nil {
			return nil, err
		}
		if result["fingerprint"] != fingerprint {
			return nil, errors.New("job_id already exists with different inputs or configuration")
		}
		if err = verifyArtifacts(final, result); err != nil {
			return nil, err
		}
		return result, nil
	}
	lock := filepath.Join(root, ".mf-"+req.JobID+".lock")
	f, err := os.OpenFile(lock, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return nil, errors.New("another worker is executing this job")
	}
	// Advisory locks are released by the OS after crashes. Keep the inode so a
	// competing retry cannot acquire a different file while this lock is held.
	defer syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	stage, err := os.MkdirTemp(root, ".mf-"+req.JobID+"-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(stage)
	results := []any{}
	data := map[string]any{}
	tags := []any{}
	for i, step := range steps {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		work := filepath.Join(stage, fmt.Sprintf("%02d-%s", i, step.Program))
		if err = os.Mkdir(work, 0700); err != nil {
			return nil, err
		}
		r := requests[i]
		r["output_dir"] = work
		if len(data) > 0 {
			r["data"] = data
		}
		result, err := Invoke(ctx, req.Programs[step.Program].Path, "run", r)
		if err != nil {
			return nil, err
		}
		if provenance, ok := result["provenance"]; ok && !reflect.DeepEqual(provenance, checks[i]) {
			return nil, errors.New("program inputs or configuration changed after preflight")
		}
		result = rewrite(result, stage, final).(map[string]any)
		results = append(results, result)
		body, ok := result["result"].(map[string]any)
		if !ok {
			return nil, errors.New("program result must be an object")
		}
		if step.DataKey != "" {
			data[step.DataKey] = body
			continue
		}
		collect := func(item map[string]any) {
			keys := make([]string, 0, len(spec.Tags))
			for key := range spec.Tags {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			for _, key := range keys {
				if value, ok := item[key]; ok {
					tag := map[string]any{"key": key, "version": spec.Tags[key], "value": value}
					if path, ok := item["media_path"]; ok {
						tag["media_path"] = path
					}
					tags = append(tags, tag)
				}
			}
		}
		collect(body)
		if items, ok := body["items"].([]any); ok {
			for _, item := range items {
				if m, ok := item.(map[string]any); ok {
					collect(m)
				}
			}
		}
	}
	artifacts := []any{}
	err = filepath.WalkDir(stage, func(path string, d os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if !d.Type().IsRegular() {
			return nil
		}
		rel, _ := filepath.Rel(stage, path)
		hash, e := Hash(path)
		if e != nil {
			return e
		}
		artifacts = append(artifacts, map[string]any{"path": rel, "sha256": hash})
		return nil
	})
	if err != nil {
		return nil, err
	}
	result := map[string]any{"job_id": req.JobID, "flow": spec.Key, "version": 1, "fingerprint": fingerprint, "output_dir": final, "tags": tags, "stages": results, "artifacts": artifacts}
	body, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return nil, err
	}
	if err = os.WriteFile(filepath.Join(stage, "result.json"), append(body, '\n'), 0600); err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if _, err = os.Lstat(final); err == nil {
		return nil, errors.New("output destination already exists")
	}
	if err = os.Rename(stage, final); err != nil {
		return nil, err
	}
	return result, nil
}
func rewrite(v any, old, new string) any {
	switch value := v.(type) {
	case string:
		if strings.HasPrefix(value, old+string(os.PathSeparator)) {
			return new + strings.TrimPrefix(value, old)
		}
	case map[string]any:
		for k, x := range value {
			value[k] = rewrite(x, old, new)
		}
	case []any:
		for i, x := range value {
			value[i] = rewrite(x, old, new)
		}
	}
	return v
}
func verifyArtifacts(root string, result map[string]any) error {
	artifacts, _ := result["artifacts"].([]any)
	for _, a := range artifacts {
		item, ok := a.(map[string]any)
		if !ok {
			return errors.New("invalid saved artifact")
		}
		rel, ok := item["path"].(string)
		if !ok || !filepath.IsLocal(rel) {
			return errors.New("invalid saved artifact path")
		}
		got, err := Hash(filepath.Join(root, rel))
		if err != nil || got != item["sha256"] {
			return fmt.Errorf("saved artifact is missing or corrupt: %s", rel)
		}
	}
	return nil
}
