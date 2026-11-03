package tributary.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Constructor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ConsumerGroupTest {
    private Topic<String> topic;
    private ConsumerGroup<String> group;

    @BeforeEach
    private void beforeEach() {
        topic = new Topic<>("topic1");
        group = topic.createConsumerGroup("group1", "roundrobin");
        group.setTopic(topic);
    }

    @Test
    public void constructorTest() {
        ConsumerGroup<String> group1 = new ConsumerGroup<>("g1", "range");
        assertEquals("g1", group1.getId());
    }

    @Test
    public void addConsumerTest() {
        Consumer<String> consumer = new SimpleConsumer("c1");
        group.addConsumer(consumer);
        assertEquals("c1", group.getConsumer("c1").getId());
    }

    @Test
    public void createConsumerTest() throws NoSuchMethodException {
        Constructor<ConstructedConsumer> constructor = ConstructedConsumer.class.getConstructor(String.class);
        group.createConsumer("c1", constructor);
        assertEquals("c1", group.getConsumer("c1").getId());
    }

    public static class ConstructedConsumer extends SimpleConsumer<String> {
        public ConstructedConsumer(String id) {
            super(id);
        }
    }

    public static class ThrowingConsumer extends Consumer<String> {
        public ThrowingConsumer(String id) {
            super(id);
            throw new IllegalStateException("constructor failure");
        }

        @Override
        public void consume(Event<String> event) {
        }
    }

    @Test
    public void reflectiveConstructionPreservesTheUnderlyingCause() throws NoSuchMethodException {
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> group.createConsumer("broken", ThrowingConsumer.class.getConstructor(String.class)));

        assertEquals(IllegalStateException.class, failure.getCause().getClass());
        assertEquals("constructor failure", failure.getCause().getMessage());
        assertNull(group.getConsumer("broken"));
    }

    @Test
    public void removeConsumerTest() {

        Consumer<String> consumer = new SimpleConsumer("c1");
        group.addConsumer(consumer);
        assertEquals("c1", group.getConsumer("c1").getId());
        group.removeConsumer("c1");
        assertNull(group.getConsumer("c1"));
    }

    @Test
    public void removingConsumerClearsItsOldAssignments() {
        topic.createPartition("p1");
        Consumer<String> removedConsumer = new SimpleConsumer<>("removed");
        Consumer<String> remainingConsumer = new SimpleConsumer<>("remaining");
        group.addConsumer(removedConsumer);
        group.addConsumer(remainingConsumer);
        assertEquals(1, removedConsumer.getPartitions().size());

        assertTrue(group.removeConsumer("removed"));

        assertTrue(removedConsumer.getPartitions().isEmpty());
        assertEquals(1, remainingConsumer.getPartitions().size());
    }

    @Test
    public void removingGroupClearsAssignmentsAndPreventsAutomaticReattachment() {
        topic.createPartition("p1");
        Consumer<String> consumer = new SimpleConsumer<>("c1");
        group.addConsumer(consumer);

        assertTrue(topic.removeConsumerGroup("group1"));
        assertTrue(consumer.getPartitions().isEmpty());
        group.rebalancing();
        topic.createPartition("p2");
        assertTrue(consumer.getPartitions().isEmpty());
    }

    @Test
    public void getInfoTest() {
        group.setRebalancing("range");
        assertEquals("Consumer group id: group1, rebalancing: Range\n", group.getInfo(0));
        group.setRebalancing("roundrobin");
        assertEquals("Consumer group id: group1, rebalancing: RoundRobin\n", group.getInfo(0));
    }
}
