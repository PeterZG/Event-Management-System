package tributary.api;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Queue;

/**
 * The Partition class represents a partition within a topic in the event-driven
 * system.
 * Each partition acts as a queue that holds events, allowing for sequential
 * processing.
 *
 * @param <T> The type of payload the events within this partition will carry.
 */
public class Partition<T> implements InfoItem {
    /**
     * The identifier for this partition.
     */
    private String id;
    /**
     * A queue that holds the events to be processed.
     */
    private final Queue<Event<T>> queue = new ArrayDeque<>();
    /**
     * A list that keeps track of events that have been "played".
     */
    private final List<Event<T>> played = new ArrayList<>();

    /**
     * Constructs a new Partition with the specified ID.
     *
     * @param id The identifier for the partition.
     */
    public Partition(String id) {
        this.id = id;
    }

    /**
     * Adds an event to the end of the queue.
     *
     * @param event The event to be added to the partition.
     */
    public synchronized void pushEvent(Event<T> event) {
        queue.add(Objects.requireNonNull(event, "event"));
    }

    /**
     * Removes and returns the event at the head of the queue.
     *
     * @return The event that was removed from the head of the queue.
     */
    public synchronized Event<T> popEvent() {
        Event<T> event = queue.poll();
        if (event != null) {
            played.add(event);
        }
        return event;
    }

    /**
     * Retrieves the identifier of this partition.
     *
     * @return The ID of the partition.
     */
    public String getId() {
        return id;
    }

    /**
     * Gets the number of events currently in the queue.
     *
     * @return The size of the queue.
     */
    public synchronized int size() {
        return queue.size();
    }

    /**
     * Provides a string representation of the partition, including the events in
     * the queue.
     *
     * @param indent The level of indentation to use for formatting the output.
     * @return A string with the partition's information, formatted with
     *         indentation.
     */
    @Override
    public synchronized String getInfo(int indent) {
        String pre = " ".repeat(indent * INDENT);
        StringBuilder sb = new StringBuilder();
        sb.append(pre).append("Partition id: ").append(id).append("\n");
        for (Event<T> next : queue) {
            sb.append(next.getInfo(indent + 1));
        }
        return sb.toString();
    }

    /**
     * Retrieves the list of events that have been played or replayed.
     *
     * @return A list of played events.
     */
    public synchronized List<Event<T>> getPlayedEvents() {
        return List.copyOf(played);
    }
}
