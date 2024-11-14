package tributary.core.reblancing;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import tributary.api.Consumer;
import tributary.api.ConsumerGroup;
import tributary.api.SimpleConsumer;
import tributary.api.Topic;

public class RebalancingTest {

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
}
