package tributary.api;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.LinkedList;

public class ConsumerTest {
    private Consumer<String> consumer;

    @BeforeEach
    public void beforeEach() {
        consumer = new SimpleConsumer("c1");
    }

    @Test
    public void constructorTest() {
        consumer = new SimpleConsumer("c2");
        assertEquals("c2", consumer.getId());
        assertEquals(0, consumer.getPartitiions().size());
    }

    @Test
    public void setPartitionsTest() {
        List<Partition<String>> parts = new LinkedList<>();
        parts.add(new Partition<>("p1"));
        assertEquals(0, consumer.getPartitiions().size());
        consumer.setPartitions(parts);
        assertEquals(1, consumer.getPartitiions().size());

        consumer.addPartition(new Partition<>("p2"));
        assertEquals(2, consumer.getPartitiions().size());
    }

    @Test
    public void addPartitionTest() {
        consumer.addPartition(new Partition<>("p1"));
        assertEquals(1, consumer.getPartitiions().size());

        consumer.addPartition(new Partition<>("p2"));
        assertEquals(2, consumer.getPartitiions().size());
    }

    @Test
    public void getInfoTest() {
        assertEquals("Consumer id: c1\n", consumer.getInfo(0));
    }
}
