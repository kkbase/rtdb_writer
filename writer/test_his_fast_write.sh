./rtdb_writer static_write \
    --plugin=../plugin_example/libcwrite_plugin.dylib \
    --static_analog=../CSV20240614/1718350759143_HISTORY_NORMAL_STATIC_ANALOG.csv \
    --static_digital=../CSV20240614/1718350759143_HISTORY_NORMAL_STATIC_DIGITAL.csv

./rtdb_writer his_fast_write \
    --plugin=../plugin_example/libcwrite_plugin.dylib \
    --his_normal_analog=../CSV20240614/1718350759143_HISTORY_NORMAL_ANALOG.csv \
    --his_normal_digital=../CSV20240614/1718350759143_HISTORY_NORMAL_DIGITAL.csv
