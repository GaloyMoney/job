NIX_DEPS_DIR := .nix-deps

.PHONY: start-deps clean-deps setup-db reset-deps sqlx-prepare check-code test-in-ci

start-deps:
	@mkdir -p $(NIX_DEPS_DIR)
	nix run .#nix-deps-base -- up -D
	nix run .#nix-deps-base -- project is-ready --wait

clean-deps:
	-nix run .#nix-deps-base -- down
	chmod -R u+w $(NIX_DEPS_DIR) 2>/dev/null || true
	rm -rf $(NIX_DEPS_DIR)

setup-db:
	nix run .#setup-db-dev

reset-deps: clean-deps start-deps

test-in-ci: start-deps
	cargo nextest run --workspace --verbose
	cargo test --doc --workspace
	cargo doc --no-deps --workspace

check-code:
	nix flake check

sqlx-prepare:
	cargo sqlx prepare --workspace
