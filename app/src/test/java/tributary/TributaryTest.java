package tributary;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

import tributary.cli.TributarySystem;

public class TributaryTest {
    @Test
    public void cliConsumesAnAssignedEventAndReportsAnEmptyPartition() {
        TributarySystem system = TributarySystem.getInstance();
        system.createTopic("cli-topic", "String");
        system.createPartition("cli-topic", "cli-partition");
        system.createConsumerGroup("cli-group", "cli-topic", "range");
        system.createConsumer("cli-group", "cli-consumer");
        system.createProducer("cli-producer", "String", "manual");
        system.produceEvent("cli-producer", "cli-topic", "events/event01.json", "cli-partition");

        assertDoesNotThrow(() -> system.consumeEvent("cli-consumer", "cli-partition"));
        assertThrows(IllegalArgumentException.class,
                () -> system.consumeEvent("cli-consumer", "cli-partition"));
    }

    @Test
    public void cliRejectsConsumptionOutsideTheConsumersAssignment() {
        TributarySystem system = TributarySystem.getInstance();
        system.createTopic("other-topic", "String");
        system.createPartition("other-topic", "other-partition");
        system.createConsumerGroup("other-group", "other-topic", "range");
        system.createConsumer("other-group", "first-consumer");
        system.createConsumer("other-group", "second-consumer");

        assertThrows(IllegalArgumentException.class,
                () -> system.consumeEvent("second-consumer", "other-partition"));
    }

    @Test
    public void parallelWorkerFailuresAreReportedToTheCaller() {
        TributarySystem system = TributarySystem.getInstance();
        system.createTopic("parallel-topic", "String");
        system.createPartition("parallel-topic", "parallel-partition");
        system.createProducer("parallel-manual", "String", "manual");

        assertThrows(IllegalArgumentException.class, () -> system.parallelProduce(
                "parallel produce (parallel-manual, parallel-topic, events/event01.json)"));
    }
}
