clean-deps:
	docker compose down

start-deps:
	@command -v docker >/dev/null 2>&1 && docker compose up -d || echo "Docker not found, skipping start-deps"

setup-db:
	cargo sqlx migrate run

reset-deps: clean-deps start-deps setup-db

test-in-ci: start-deps setup-db
	cargo nextest run --workspace --verbose
	cargo test --doc --workspace
	cargo doc --no-deps --workspace

check-code:
	SQLX_OFFLINE=true cargo fmt --check --all
	SQLX_OFFLINE=true cargo check --workspace
	SQLX_OFFLINE=true cargo clippy --workspace --all-features
	# temporarily dissabled due to toml version issue
	# SQLX_OFFLINE=true cargo audit 
	# SQLX_OFFLINE=true cargo deny check

sqlx-prepare:
	cargo sqlx prepare --workspace
