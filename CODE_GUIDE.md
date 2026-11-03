# Tributary 代码导览

## 模块

| 路径 | 职责 |
| --- | --- |
| `app/src/main/java/tributary/api/` | Event、Partition、Topic、Producer、Consumer 和 ConsumerGroup |
| `app/src/main/java/tributary/core/` | 生产者实现 |
| `app/src/main/java/tributary/core/rebalancing/` | Range、RoundRobin 与策略工厂 |
| `app/src/main/java/tributary/cli/TributaryCLI.java` | 命令解析和交互入口 |
| `app/src/main/java/tributary/cli/Cluster.java` | 内存注册表 |
| `app/src/test/` | 单元和并发测试 |
| `app/events/` | 示例事件数据 |
| `config/` | Checkstyle 规则 |

## 阅读顺序

先看 Partition 的队列与历史，再看 Topic 如何组织分区，以及 ConsumerGroup 如何分配分区。最后通过 CLI 了解对象的创建和使用方式。

## 消费语义

Partition 是共享的破坏式 FIFO。一次消费会取走事件；不同消费者组独立的是分区分配，不是消息副本或独立读取位点。

played-event 历史是不可变快照。空队列返回 null，不向历史追加空事件。Range 分配连续分区，RoundRobin 轮流分配；消费者或分区变化时执行重平衡。

## 同步与错误

分区的队列和历史操作在同一同步边界内。反射创建消费者时保留异常原因，便于判断构造失败。

配置整个拓扑时按顺序操作；分区级线程安全不表示所有注册表和拓扑修改都支持任意并发。

移除消费者、消费者组或主题会撤销旧分区引用。Topic 在释放自身锁后才通知组脱离，避免与重平衡时“先组、后主题”的锁顺序相反。JSON 负载先按 UTF-8 读取并验证完整整数范围，再交给生产者，不依赖会截断小数的 `JSONObject.getInt`。

## 维护约定

说明先给出完整的发布、消费示例，再列命令参考。示例使用稳定名称，解释文件的相对位置；占位符用尖括号标记，避免被误当作需要原样输入的内容。

这是 Java 库与 CLI 项目，源码统一在 `app/src/`。类名与文件名一致，策略实现独立于公共模型。注释说明消费语义、同步边界和兼容入口，遵循现有 Checkstyle。

[运行和命令](README.md)
