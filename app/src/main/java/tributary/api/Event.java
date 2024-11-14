package tributary.api;

import tributary.core.Header;

/**
 * The Event class represents an event in the event-driven system.
 *
 * @param <T> The type of payload the event will carry.
 */
public class Event<T> implements InfoItem {
    /**
     * The header containing metadata about the event.
     */
    private Header header;
    /**
     * An optional key which can be used for partitioning.
     */
    private String key;
    /**
     * The payload or content of the event.
     */
    private T value;

    /**
     * Constructs a new Event with the specified ID, key, and payload.
     *
     * @param id    The unique identifier for the event.
     * @param key   An optional key for the event.
     * @param value The payload or content of the event.
     */
    public Event(String id, String key, T value) {
        this.header = new Header(id);
        this.key = key;
        this.value = value;
    }

    /**
     * Constructs a new Event with the specified header, key, and payload.
     *
     * @param header The header containing metadata about the event.
     * @param key    An optional key for the event.
     * @param value  The payload or content of the event.
     */
    public Event(Header header, String key, T value) {
        this.header = header;
        this.key = key;
        this.value = value;
    }

    /**
     * Retrieves the header of the event.
     *
     * @return The header of the event.
     */
    public Header getHeader() {
        return header;
    }

    /**
     * Retrieves the optional key associated with the event.
     *
     * @return The key of the event, or null if no key is set.
     */
    public String getKey() {
        return key;
    }

    /**
     * Sets the optional key associated with the event.
     *
     * @param key The key to be associated with the event.
     */
    public void setKey(String key) {
        this.key = key;
    }

    /**
     * Retrieves the payload or content of the event.
     *
     * @return The payload of the event.
     */
    public T getValue() {
        return value;
    }

    /**
     * Retrieves the identifier of the event from the header.
     *
     * @return The ID of the event.
     */
    public String getId() {
        return header.getId();
    }

    /**
     * Provides a string representation of the event.
     *
     * @param indent The level of indentation.
     * @return A string with the event's information.
     */
    @Override
    public String getInfo(int indent) {
        String pre = " ".repeat(indent * INDENT);
        StringBuilder sb = new StringBuilder();
        sb.append(pre).append("Event id: ").append(getId()).append("\n");
        sb.append(pre).append("Event content: ").append(value).append("\n");
        return sb.toString();
    }

}
