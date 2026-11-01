# Consolidation fixtures

`one.ir` and `two.ir` contain real CLP KV IR generated from the adjacent `.log` files with
`ghcr.io/y-scope/clp/clp-package:0.14.1` (digest
`sha256:f04d30e9243b2af0e549f2e81e547c0b9640d4e9f02c00aed34771987115671a`):

```bash
docker run --rm --entrypoint /opt/clp/bin/log-converter \
  -v "$PWD/tests/fixtures/consolidation:/fixtures" \
  ghcr.io/y-scope/clp/clp-package:0.14.1 \
  --no-compress-converted-files --output-dir /fixtures /fixtures/one.log /fixtures/two.log
```

Rename the generated UUID files to `one.ir`/`two.ir` using their `original_file` IR metadata.
Tests compress these IR streams and extract four original records with the real engine. The `.jsonl`
files exercise the separate FILE-to-archive chain.

`generate.go` uses the repository's pinned Go MessagePack/LZ4 codec to produce `go-payload.lz4`,
`go-result.lz4` (no error key), and `go-error.lz4`. Regenerate from the repository root with
`go run tests/fixtures/consolidation/generate.go`.
