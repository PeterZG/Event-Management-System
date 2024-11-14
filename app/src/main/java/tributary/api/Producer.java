package tributary.api;

/**
 * The Producer class serves as an abstract representation of a producer in the
 * event-driven system. A producer is responsible for creating and sending
 * events to a topic.
 *
 * @param <T> The type of payload the produced events will carry.
 */
public abstract class Producer<T> {
    /**
     * The identifier for this producer instance.
     */
    private String id;

    /**
     * Constructs a new Producer with the specified ID.
     *
     * @param id The identifier for the producer.
     */
    protected Producer(String id) {
        this.id = id;
    }

    /**
     * Retrieves the identifier of this producer.
     *
     * @return The ID of the producer.
     */
    public String getId() {
        return id;
    }

    /**
     * Produces an event with the given ID, key, and payload, and sends it to the
     * appropriate topic.
     *
     * @param id    The unique identifier for the event.
     * @param key   An optional key for the event
     * @param value The content to be sent as part of the event.
     * @return The event that was produced.
     */
    public abstract Event<T> produce(String id, String key, T value);

    /**
     * Allocates a partition for the given event within the specified topic.
     * The allocation strategy can be based on the event's key or other criteria
     * defined by the producer.
     *
     * @param event The event for which a partition is to be allocated.
     * @param topic The topic to which the event will be sent.
     * @return The partition id to which the event should be sent.
     */
    public abstract String allocate(Event<T> event, Topic<T> topic);

}
