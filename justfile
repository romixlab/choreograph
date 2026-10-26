# choreograph: library, CLI (and later the GUI)

# Bare `just` only lists the recipes
default:
    @just --list

# The tests. `just test`: all of them (test-all). `just test --changed`: only the workspace crates changed against
# main and the crates that depend on them (needs tpm on PATH).
test *flags:
    if [ "{{flags}}" = "--changed" ]; then tpm test changed -- cargo nextest run --workspace; else {{just_executable()}} test-all {{flags}}; fi

test-all *flags:
    cargo nextest run --workspace --all-features --no-tests=pass {{flags}}
    cargo test --workspace --all-features --doc

lint:
    cargo clippy --workspace --all-targets --all-features -- -D warnings
    cargo fmt --all --check
    # no C: no -sys crate, no cc, cmake or bindgen in the build (AGENTS.md)
    ! cargo tree --workspace --all-features -e normal,build --prefix none | rg -q '^(cc|cmake|bindgen|[a-z0-9_-]+-sys) '

# Build and put the choreograph and choreographd binaries on PATH once: through tpm install when tpm is there,
# else cargo install
install:
    cargo build --profile deploy --locked -p choreograph_cli -p choreographd
    if command -v tpm >/dev/null; then tpm install target/deploy/choreograph target/deploy/choreographd; else cargo install --locked --path choreograph_cli && cargo install --locked --path choreographd; fi

# Bring this checkout up to main and install
deploy:
    test -z "$(git status --porcelain)" || { echo "local changes, commit or move them first:" >&2; git status --short >&2; exit 1; }
    git pull --ff-only
    just install
    choreograph --version | grep -qF "($(git rev-parse --short HEAD)"
    choreographd --version | grep -qF "($(git rev-parse --short HEAD)"
    choreograph --version
