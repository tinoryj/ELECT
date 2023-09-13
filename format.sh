#!/bin/bash

# 遍历目录下的所有.java文件
find . -name "*.java" | while read -r file
do
  # 使用 prettier 进行格式化
  prettier --write "$file" --plugin-search-dir=.
done

