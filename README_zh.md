# 事件管理系统

## 介绍

本项目是一个事件管理系统，模拟了生产者-消费者模型。通过灵活的分区、消费者组和再平衡策略，展示了事件消息的高效处理和分发。核心目标是提供一个可扩展的面向对象设计结构，更好地支持分布式消息处理。

## 技术框架

- **编程语言**：Java  
- **设计模式**：工厂模式，策略模式  
- **核心技术**：消息队列管理，分布式事件处理  
- **数据结构**：Map，LinkedList 等  
- **构建工具**：Gradle  

## 快速开始

### 前提条件

- Java JDK 17  
- Gradle  
- Git  

### 安装

1. 克隆代码库：

```bash
git clone [repository-link]
```

2. 进入项目目录并构建：

```bash
cd [your directory path]
gradle build
```

3. 运行项目：

```bash
gradle run
```

### 使用示例

1. 生成事件：使用 `RandomProducer` 或 `ManualProducer` 创建事件。  
2. 分配事件：根据分区策略（如 Range 或 RoundRobin）将事件分发给消费者。  
3. 消费事件：消费者组根据指定的偏移量或策略从分区中消费消息。  

## 文件结构

- **app**：核心应用逻辑  
- **config**：配置文件和设置  
- **design**：包括类图设计及相关文档  
- **blog**：开发过程记录和博客  
- **README.md**：项目文档  

## 联系方式

如有任何疑问，请联系：

- Weihou Zeng: weihouzeng@gmail.com 或 849997616@qq.com  

## 作者

- **Weihou Zeng** - *全栈开发与设计*  

## 致谢

本项目是我独立努力和探索的结果。特别感谢开源社区在整个开发过程中提供的宝贵资源和灵感。

