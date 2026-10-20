# mf-flow-infer-custom-speakers

Diarize recordings and compare each local speaker embedding with every supplied compatible voice profile. An explicit cosine similarity threshold is mandatory. All candidate scores are returned; below-threshold or tied matches remain unknown. Scores are not probabilities.

Standalone macOS ARM64 flow using the mf version-1 stdin/stdout envelope. Dependencies: pyannote@1. No mf-hub server or client database implementation is linked into this executable. The supplied client database path is checked for existence but never opened or written.

## Run

Install and explicitly prepare the dependency programs. Edit `example-request.json` with existing absolute media, program, database and output-directory paths.

```sh
bin/infer_custom_speakers describe
bin/infer_custom_speakers preflight < example-request.json
bin/infer_custom_speakers < example-request.json
```

`options` configures the final program operation. `program_options.<name>` configures a particular dependency; `describe` on that program exposes its complete option schema. Multiple audio streams require media objects with explicit `stream_index`; optional start/end times are in seconds. Media is local to the executing machine. Every selected stream is fully decoded during preflight before any inference starts. Missing files, corrupt media, missing models, unsupported options or incompatible profiles abort the entire job.

Results are published atomically at `<input.output_dir>/<job_id>/result.json` with versioned tags, complete stage responses, source/model provenance, and hashed artifacts. Intermediate writes stay in a staging directory. SIGINT/SIGTERM terminates subprocess groups and removes active staging output. Stable job IDs and content/configuration fingerprints make identical retries return the saved result; conflicting retries fail. Artifact checks detect corrupted prior results. Uncatchable kills can leave unpublished `.mf-*` staging directories, which may be cleaned when the matching advisory lock is no longer held.

## Build

```sh
go test ./...
go vet ./...
python3 scripts/release.py --repo ariym/mf-flow-infer-custom-speakers --version 1 --platform darwin/arm64
```

The source manifest is `flow.json`; generated `dist/manifest.json` has real checksums and release URLs. Only validated darwin/arm64 artifacts are published. Source is based on mf-hub's canonical flow template and its reusable `engine/` package. No installation adapter or client tag storage is implemented here.
