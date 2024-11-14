package tributary.api;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import tributary.core.Header;

public class EventTest {

    @Test
    public void constructorTest() {
        Event<String> event = new Event<>("event01", "p1", "event");
        assertEquals("p1", event.getKey());
        assertEquals("event", event.getValue());
    }

    @Test
    public void constructor2Test() {
        Header header = new Header("event01");
        Event<String> event = new Event<>(header, "p1", "event");
        assertEquals("p1", event.getKey());
        assertEquals("event", event.getValue());
    }

    @Test
    public void getIdTest() {
        Event<String> event = new Event<>("event01", "p1", "event");
        assertEquals("event01", event.getId());
        assertEquals("event01", event.getHeader().getId());
    }

    @Test
    public void setKeyTest() {
        Event<String> event = new Event<>("event01", "p1", "event");
        assertEquals("p1", event.getKey());
        event.setKey("p2");
        assertEquals("p2", event.getKey());
    }

    @Test
    public void getInfoTest() {
        Event<String> event = new Event<>("event01", "p1", "event");
        assertEquals("Event id: event01\nEvent content: event\n", event.getInfo(0));
    }
}
