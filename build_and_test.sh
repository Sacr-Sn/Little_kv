#!/bin/bash

set -e

mkdir -p build
cd build
cmake ..
make -j$(nproc)

echo "🔧 运行主程序"
./app/main_kv

# echo "🧪 运行测试"
# ctest --output-on-failure
