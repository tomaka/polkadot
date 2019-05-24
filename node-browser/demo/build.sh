cd ..
wasm-pack build --target web --out-dir ./demo/pkg --no-typescript --release
cd demo
xdg-open index.html
