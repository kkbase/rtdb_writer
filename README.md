# rtdb_writer

实时/时序数据库的写入测试程序

本程序由两个部分组成: 
* 主体部分: 负责读取csv文件, 并且按照一定测试规则调用数据发送插件
* 插件部分: 由各个厂商需自己实现基于```plubin/write_plugin.h```头文件的插件

# 目录结构
```tex
.
├── LICENSE
├── README.md
├── plugin
│   └── write_plugin.h  // 插件头文件
├── plugin_example      // 插件示例
└── writer              // 写入器(由golang实现)

```

# writer设计图
![img.png](resource/design_drawing.png)

# 周期性写入, 写入流程说明
![img.png](resource/periodic_write_process.png)

由于**快采点**和**普通点**写入周期不同, 所以开启了两个协程序分别进行**快采点**和**普通点**的写入, 在写入方面**快采点**和**普通点**互不影响.
但是由于**快采点**和**普通点**共用一个插件, 所以要求在插件实现的写入接口是可重入的. 