cd ..
wasm-pack build --target web --out-dir ./demo/pkg --no-typescript --release
cd demo
python -m SimpleHTTPServer 8000
