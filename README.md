# pymscada-rust
Rust client for pymscada - intended to allow rust binaries submit data to the bus

## Command line snips

```
cargo test test_collect_samples -- --nocapture
cargo run -- --fft --verbose
cargo build --release
sudo cp target/release/pymscada-rust /usr/local/bin
nohup pymscada-rust --fft > run.log 2>&1 &
```
