go build -o rtdb_writer

# 如果缺少库文件, 可以使用 -l 选项自主添加缺少的库文件
# go build -ldflags '-extldflags "-ldl"' -o rtdb_writer