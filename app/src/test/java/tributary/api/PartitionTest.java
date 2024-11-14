package tributary.api;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PartitionTest {
    private Partition<String> partition;

    @BeforeEach
    public void beforeEach() {
        partition = new Partition<>("p1");
    }

    @Test
    public void constructorTest() {
        partition = new Partition<>("p1");
        assertEquals("p1", partition.getId());
    }

    @Test
    public void pushEventTest() {
        Event<String> event = new Event<String>("e1", null, "event");
        partition.pushEvent(event);
        assertEquals(1, partition.size());
    }

    @Test
    public void popEventTest() {
        Event<String> event = new Event<String>("e1", null, "event");
        partition.pushEvent(event);
        assertEquals(1, partition.size());
        Event<String> eventPop = partition.popEvent();
        assertEquals(0, partition.size());
        List<Event<String>> playedEvents = partition.getPlayedEvents();
        assertEquals(1, playedEvents.size());
    }

    @Test
    public void getInfoTest() {

        Event<String> event = new Event<String>("e1", null, "event");
        partition.pushEvent(event);
        assertEquals("Partition id: p1\n  Event id: e1\n  Event content: event\n", partition.getInfo(0));
    }
}
