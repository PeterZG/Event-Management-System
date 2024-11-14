package tributary.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TopicTest {
    private Topic<String> topic;

    @BeforeEach
    private void beforeEach() {
        topic = new Topic<>("t1");
    }

    @Test
    public void constructorTest() {
        Topic<String> t1 = new Topic<>("t1");
        assertEquals("t1", t1.getId());
    }

    @Test
    public void createPartitionTest() {
        assertEquals(0, topic.getPartitions().size());
        topic.createPartition("p1");
        assertEquals(1, topic.getPartitions().size());
        Partition<String> part = topic.getPartition("p1");
        assertEquals("p1", part.getId());
    }

    @Test
    public void createPartitionExceptionTest() {
        assertEquals(0, topic.getPartitions().size());
        topic.createPartition("p1");
        assertThrows(IllegalArgumentException.class, () -> topic.createPartition("p1"));
        assertEquals(1, topic.getPartitions().size());
    }

    @Test
    public void createGroupTest() {
        ConsumerGroup<String> group = topic.createConsumerGroup("g1", "range");
        assertEquals("g1", group.getId());
    }

    @Test
    public void createGroupExceptionTest() {
        ConsumerGroup<String> group = topic.createConsumerGroup("g1", "range");
        assertEquals("g1", group.getId());
        assertThrows(IllegalArgumentException.class, () -> topic.createConsumerGroup("g1", "range"));
    }

    @Test
    public void getInfoTest() {
        assertEquals("Topic id: t1\n", topic.getInfo(0));
    }
}
