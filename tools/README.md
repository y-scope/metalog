# Code Formatting Tools

## Usage

### Format all files
```bash
./scripts/format.sh
```

### Format specific files
```bash
./tools/gjf.sh src/main/java/com/yscope/metalog/node/Node.java
```

Google Java Format and Java 21 are auto-installed on first run to `tools/bin/` (gitignored).

## Updating

To update to a new version, edit `GJF_VERSION` in `tools/gjf.sh`, then delete the old jar:

```bash
rm -rf tools/bin/google-java-format-*.jar
./tools/gjf.sh src/main/java/com/yscope/metalog/node/Node.java  # re-downloads
```
