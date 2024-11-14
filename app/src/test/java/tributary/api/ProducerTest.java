package tributary.api;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import tributary.core.ManualProducer;
import tributary.core.RandomProducer;

public class ProducerTest {
    private ProducerFactory<String> factory;

    @BeforeEach
    public void beforeEach() {
        factory = new ProducerFactory<>();
    }

    @Test
    public void createTest() {
        Producer<String> p1 = factory.create("p1", "random");
        assertEquals("p1", p1.getId());
        assertTrue(p1 instanceof RandomProducer);
    }

    @Test
    public void createTest2() {
        Producer<String> p1 = factory.create("p1", "manual");
        assertEquals("p1", p1.getId());
        assertTrue(p1 instanceof ManualProducer);
    }

    @Test
    public void createExceptionTest() {
        assertThrows(IllegalArgumentException.class, () -> factory.create("p1", "notExist"));
    }

    @Test
    public void randomProducerTest() {
        Topic<String> topic = new Topic<>("t1");
        Producer<String> p1 = factory.create("produce01", "random");
        Event<String> event = p1.produce("event01", null, "event");
        assertNull(event.getKey());
        assertEquals("event01", event.getId());

        assertThrows(IllegalArgumentException.class, () -> p1.allocate(event, topic));

        topic.createPartition("p1");
        assertDoesNotThrow(() -> p1.allocate(event, topic));
    }

    @Test
    public void manualProducerTest() {
        Topic<String> topic = new Topic<>("t1");
        topic.createPartition("part01");
        Producer<String> p1 = factory.create("produce01", "manual");

        assertThrows(IllegalArgumentException.class, () -> p1.produce("event01", null, "event"));

        Event<String> event = p1.produce("event01", "part01", "event");
        assertEquals("event01", event.getId());
        assertEquals("part01", event.getKey());

        event.setKey(null);
        assertThrows(IllegalArgumentException.class, () -> p1.allocate(event, topic));
        event.setKey("notExist");
        assertThrows(IllegalArgumentException.class, () -> p1.allocate(event, topic));

        event.setKey("part01");
        assertDoesNotThrow(() -> p1.allocate(event, topic));
    }
}
