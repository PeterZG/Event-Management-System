package tributary.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

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
    public void createConsumerTest() {

        Topic topic = new Topic<>("topic1");
        ConsumerGroup group = topic.createConsumerGroup("group1", "roundrobin");
        group.setTopic(topic);
        Constructor<SimpleConsumer> c;
        try {
            c = SimpleConsumer.class.getConstructor(String.class);
            group.createConsumer("c1", c);
            assertEquals("c1", group.getConsumer("c1").getId());
        } catch (Exception e) {
            e.printStackTrace();
        }
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
    public void getInfoTest() {
        group.setRebalancing("range");
        assertEquals("Consumer group id: group1, rebalancing: Range\n", group.getInfo(0));
        group.setRebalancing("roundrobin");
        assertEquals("Consumer group id: group1, rebalancing: RoundRobin\n", group.getInfo(0));
    }
}
