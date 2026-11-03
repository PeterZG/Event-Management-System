# Tributary — Java 事件代理模拟器

用于理解主题、分区、生产者、消费者和消费者组重平衡的 Java 命令行项目。

维护者：**Weihou Zeng（PeterZG）**。

## 功能与语义

支持类型化主题、FIFO 分区、手动/随机生产者分配，以及 Range/RoundRobin 重平衡。

不同消费者组有独立的分区分配，但共享同一个破坏式队列。消息被取走后，其他组不能通过独立 offset 再读一次。已消费事件历史可用于查看和回放，不是持久化消息日志。

## 目录

- `app/src/main/java/tributary/api/`：公共模型。
- `app/src/main/java/tributary/core/`：生产者和重平衡策略。
- `app/src/main/java/tributary/cli/`：命令行与注册表。
- `app/src/test/`：行为和并发测试。
- `app/events/`：事件 JSON 示例。

## 启动

需要 JDK 17+：

```sh
./gradlew run --console=plain
```

Windows 使用 `gradlew.bat`。输入 `help` 查看命令，输入 `exit` 退出。

可创建主题、分区、消费者组和生产者，再发布、消费或回放事件。完整命令格式见 [英文 README](README.md)。

### 发送第一条消息

Gradle 的运行任务在 `app/` 下启动，示例文件位于其中的 `events/` 文件夹。启动后依次输入：

```text
create topic updates String
create partition updates inbox
create consumer group readers updates range
create consumer readers reader
create producer writer String manual
produce event writer updates events/event01.json inbox
consume event reader inbox
exit
```

生产者把 `Event01` 放入 `inbox`，消费者会显示消息编号和内容。命令参考中 `<...>` 表示需要替换的参数，不要连尖括号一起输入。

## 命令与数据规则

可读取消费者和生产者，切换生产者分配策略，或删除主题、分区、消费者组、消费者和生产者。
删除主题会同时删除其消费者组；删除分区会丢弃该分区队列并重平衡。主题和生产者的负载类型必须匹配，支持 String 和 Integer。
删除消费者、组或主题时，会清空已脱离消费者的旧分区分配。事件文件按 UTF-8 读取；Integer 必须是 32 位有符号范围内的整数 JSON 数值，拒绝小数、越界数值和数字字符串，不会静默截断或溢出。
分区、消费者组和消费者 ID 在 CLI 集群内唯一。并行操作会向调用者报告失败，但不会回滚已成功的任务。

## 测试

```sh
./gradlew test lint assemble
./gradlew jacocoTestReport
```

覆盖率报告位于 `app/build/reports/jacoco/`。

GitHub 流水线在 Java 17 下执行测试、风格检查、构建和覆盖率。保留的 GitLab 文件属于原课程运行器，两者都不部署服务；本地检查不能代替远程 CI 结论。

## 限制

数据只保存在进程内存中，不提供网络传输、复制、持久化或分布式恢复。分区访问有同步保护，整个集群的拓扑配置应按顺序执行。

[English](README.md) · [代码导览](CODE_GUIDE.md)
