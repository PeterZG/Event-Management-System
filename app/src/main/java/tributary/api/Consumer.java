package tributary.api;

import java.util.LinkedList;
import java.util.List;

/**
 * The Consumer class represents an entity in the event-driven system that is
 * capable of consuming (processing) events from one or more partitions.
 *
 * @param <T> The type of payload the events will carry.
 */
public abstract class Consumer<T> implements InfoItem {
    /**
     * The identifier for this consumer.
     */
    private String id;
    /**
     * A list of partitions that this consumer is subscribed to.
     */
    private List<Partition<T>> partitions;

    /**
     * Constructs a new Consumer with the specified ID.
     *
     * @param id The identifier for the consumer.
     */
    protected Consumer(String id) {
        this.id = id;
        partitions = new LinkedList<>();
    }

    /**
     * Consumes an event by processing it according to the consumer's specific
     * logic.
     *
     * @param event The event to be consumed.
     */
    public abstract void consume(Event<T> event);

    /**
     * Retrieves the identifier of this consumer.
     *
     * @return The ID of the consumer.
     */
    public String getId() {
        return id;
    }

    /**
     * Retrieves the list of partitions that this consumer is subscribed to.
     *
     * @return A list of partitions.
     */
    public List<Partition<T>> getPartitiions() {
        return partitions;
    }

    /**
     * Sets the list of partitions that this consumer is subscribed to.
     *
     * @param partitions A list of partitions.
     */
    public void setPartitions(List<Partition<T>> partitions) {
        this.partitions = partitions;
    }

    /**
     * Adds a partition to the list of partitions of this consumer
     *
     * @param partition The partition to be added.
     */
    public void addPartition(Partition<T> partition) {
        partitions.add(partition);
    }

    /**
     * Provides a string representation of the consumer, including the partitions it
     * is subscribed to.
     *
     * @param indent The level of indentation to use for formatting the output.
     * @return A string with the consumer's information, formatted with indentation.
     */
    public String getInfo(int indent) {
        String pre = " ".repeat(indent * INDENT);
        StringBuilder sb = new StringBuilder();
        sb.append(pre).append("Consumer id: ").append(id).append("\n");
        String subPre = " ".repeat((indent + 1) * INDENT);
        partitions.stream().forEach(p -> sb.append(subPre).append("Partition id: ").append(p.getId()).append("\n"));
        return sb.toString();
    }

}
