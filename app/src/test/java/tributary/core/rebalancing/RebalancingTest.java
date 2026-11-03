package tributary.core.rebalancing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

import tributary.api.Consumer;
import tributary.api.ConsumerGroup;
import tributary.api.SimpleConsumer;
import tributary.api.Topic;

public class RebalancingTest {

    @Test
    public void deletingTheLastPartitionClearsAssignmentsForBothStrategies() {
        for (String strategy : List.of("range", "roundrobin")) {
            Topic<String> topic = new Topic<>("topic");
            topic.createPartition("last");
            ConsumerGroup<String> group = topic.createConsumerGroup("group", strategy);
            Consumer<String> consumer = new SimpleConsumer<>("consumer");
            group.addConsumer(consumer);
            assertEquals(1, consumer.getPartitions().size());

            assertTrue(topic.removePartition("last"));

            assertTrue(consumer.getPartitions().isEmpty());
        }
    }

    @Test
    public void createTest() {
        RebalancingStrategyFactory factory = new RebalancingStrategyFactory();
        RebalancingStrategy s1 = factory.create("range");
        assertTrue(s1 instanceof RangeStrategy);
        RebalancingStrategy s2 = factory.create("roundrobin");
        assertTrue(s2 instanceof RoundRobinStrategy);

        assertThrows(IllegalArgumentException.class, () -> factory.create("notExist"));
    }

    @Test
    public void rangeTest() {
        Topic<String> topic = new Topic("topic");
        topic.createPartition("p1");
        topic.createPartition("p2");
        topic.createPartition("p3");
        topic.createPartition("p4");
        topic.createPartition("p5");

        ConsumerGroup<String> group = topic.createConsumerGroup("g1", "range");
        group.setTopic(topic);
        Consumer<String> consumer = new SimpleConsumer<String>("c1");
        group.addConsumer(consumer);
        Consumer<String> consumer2 = new SimpleConsumer<String>("c2");
        group.addConsumer(consumer2);

        assertEquals(3, consumer.getPartitiions().size());
        assertEquals(2, consumer2.getPartitiions().size());
    }

    @Test
    public void roundrobinTest() {
        Topic<String> topic = new Topic("topic");
        topic.createPartition("p1");
        topic.createPartition("p2");
        topic.createPartition("p3");
        topic.createPartition("p4");
        topic.createPartition("p5");

        ConsumerGroup<String> group = topic.createConsumerGroup("g1", "roundrobin");
        group.setTopic(topic);
        Consumer<String> consumer = new SimpleConsumer<String>("c1");
        group.addConsumer(consumer);
        Consumer<String> consumer2 = new SimpleConsumer<String>("c2");
        group.addConsumer(consumer2);

        assertEquals(3, consumer.getPartitiions().size());
        assertEquals(2, consumer2.getPartitiions().size());
    }

    @Test
    public void partitionsAddedAfterConsumersAreAssignedPredictably() {
        Topic<String> topic = new Topic<>("topic");
        ConsumerGroup<String> group = topic.createConsumerGroup("g1", "roundrobin");
        Consumer<String> first = new SimpleConsumer<>("c1");
        Consumer<String> second = new SimpleConsumer<>("c2");
        group.addConsumer(first);
        group.addConsumer(second);

        topic.createPartition("p1");
        topic.createPartition("p2");
        topic.createPartition("p3");

        assertEquals(List.of("p1", "p3"),
                first.getPartitions().stream().map(p -> p.getId()).toList());
        assertEquals(List.of("p2"),
                second.getPartitions().stream().map(p -> p.getId()).toList());

        group.removeConsumer("c1");
        assertEquals(List.of("p1", "p2", "p3"),
                second.getPartitions().stream().map(p -> p.getId()).toList());
    }

    @Test
    public void duplicateConsumerIdentifiersAreRejected() {
        Topic<String> topic = new Topic<>("topic");
        ConsumerGroup<String> group = topic.createConsumerGroup("g1", "range");
        group.addConsumer(new SimpleConsumer<>("c1"));

        assertThrows(IllegalArgumentException.class,
                () -> group.addConsumer(new SimpleConsumer<>("c1")));
    }
}
