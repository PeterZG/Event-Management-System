
# Event Management System

## Introduction

This project is an Event Management System that simulates the producer-consumer model. Through flexible partitioning, consumer groups, and rebalancing strategies, it demonstrates efficient handling and distribution of message events. The core focus is to provide a scalable, object-oriented design structure to better support distributed message processing.

## Technical Framework

- **Programming Language**: Java
- **Design Patterns**: Factory Pattern, Strategy Pattern
- **Core Technologies**: Message Queue Management, Distributed Event Processing
- **Data Structures**: Map, LinkedList, etc.
- **Build Tool**: Gradle

## Getting Started

### Prerequisites

- Java JDK 17
- Gradle
- Git

### Installation

1. Clone the repository:

```bash
git clone [repository-link]
```

2. Navigate to the project directory and build:

```bash
cd assignment-iii-main
gradle build
```

3. Run the project:

```bash
gradle run
```

### Usage Examples

1. Generate events: Use `RandomProducer` or `ManualProducer` to create events.
2. Allocate events: Distribute events to consumers based on partition strategies (e.g., Range or RoundRobin).
3. Consume events: Consumer groups consume messages from partitions using specified offsets or strategies.

## File Structure

- **app/**: Core application logic
- **config/**: Configuration files and settings
- **design/**: Includes class diagram designs and related documents
- **blog/**: Development process records and blogs
- **README.md**: Project documentation

## Contact

For any inquiries, please contact:

- Weihou Zeng: z5270202@ad.unsw.edu.au

## Authors

- **Weihou Zeng** - *Backend Development & Design*

## Acknowledgments

Special thanks to the COMP2511 course team at UNSW for their guidance and resources.
