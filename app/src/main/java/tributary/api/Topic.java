package tributary.api;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Represents a topic in the event-driven system, which is a logical grouping of
 * related events.
 *
 * @param <T> The type of payload the events in this topic will carry.
 */
public class Topic<T> implements InfoItem {
    /**
     * The identifier for this topic.
     */
    private String id;
    /**
     * A map of consumer groups associated with this topic
     */
    private Map<String, ConsumerGroup<T>> groups = new HashMap<>();
    /**
     * A map of partitions of topic
     */
    private Map<String, Partition<T>> partitions = new LinkedHashMap<>();

    /**
     * Constructs a new Topic with the specified ID.
     *
     * @param id The identifier for the topic.
     */
    public Topic(String id) {
        this.id = id;
    }

    /**
     * Retrieves the map of partitions associated with this topic.
     *
     * @return map of the partitions in this topic.
     */
    public Map<String, Partition<T>> getPartitions() {
        return partitions;
    }

    /**
     * Retrieves the ID of this topic.
     *
     * @return The ID of the topic.
     */
    public String getId() {
        return id;
    }

    /**
     * Creates a new partition within this topic with the specified ID.
     *
     * @param id The id for the new partition.
     * @throws IllegalArgumentException If a partition with the same ID already
     *                                  exists.
     */
    public void createPartition(String id) {
        if (partitions.get(id) != null) {
            throw new IllegalArgumentException("Partition already exists. id: " + id);
        }
        partitions.put(id, new Partition<>(id));
    }

    /**
     * Creates a new consumer group within this topic with the specified ID and
     * rebalancing strategy.
     *
     * @param id          The id for the new consumer group.
     * @param rebalancing The initial rebalancing strategy
     * @return The newly created consumer group.
     * @throws IllegalArgumentException If a consumer group with the same ID already
     *                                  exists.
     */
    public ConsumerGroup<T> createConsumerGroup(String id, String rebalancing) {
        if (groups.get(id) != null) {
            throw new IllegalArgumentException("ConsumerGroup already exists. id: " + id);
        }
        ConsumerGroup<T> group = new ConsumerGroup<>(id, rebalancing);
        groups.put(id, group);
        return group;
    }

    /**
     * Retrieves a partition by its ID from this topic.
     *
     * @param id The ID of the partition to retrieve.
     * @return The partition if it exists, or null if no such partition exists.
     */
    public Partition<T> getPartition(String id) {
        return partitions.get(id);
    }

    /**
     * Provides a string representation of the topic, including its partitions and
     * their events.
     *
     * @param indent The level of indentation to use for formatting the output.
     * @return A string with the topic's information, formatted with indentation.
     */
    @Override
    public String getInfo(int indent) {
        String pre = " ".repeat(indent * INDENT);
        StringBuilder sb = new StringBuilder();
        sb.append(pre).append("Topic id: ").append(id).append("\n");
        partitions.values().stream().forEach(p -> sb.append(p.getInfo(indent + 1)));
        return sb.toString();
    }
}
