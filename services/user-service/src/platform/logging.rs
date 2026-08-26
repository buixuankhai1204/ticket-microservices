use tracing_subscriber::EnvFilter;

pub fn init() {
    // EnvFilter::from_default_env() emits nothing at all when RUST_LOG is unset,
    // rather than defaulting to a sane level — set one explicitly.
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::fmt()
        .json()
        .with_env_filter(filter)
        .init();
}
