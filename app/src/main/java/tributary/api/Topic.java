package tributary.api;

import java.util.Collections;
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
    private final String id;
    /**
     * A map of consumer groups associated with this topic
     */
    private final Map<String, ConsumerGroup<T>> groups = new LinkedHashMap<>();
    /**
     * A map of partitions of topic
     */
    private final Map<String, Partition<T>> partitions = new LinkedHashMap<>();

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
    public synchronized Map<String, Partition<T>> getPartitions() {
        return Collections.unmodifiableMap(new LinkedHashMap<>(partitions));
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
        Map<String, ConsumerGroup<T>> groupsSnapshot;
        synchronized (this) {
            if (partitions.containsKey(id)) {
                throw new IllegalArgumentException("Partition already exists. id: " + id);
            }
            partitions.put(id, new Partition<>(id));
            groupsSnapshot = new LinkedHashMap<>(groups);
        }
        groupsSnapshot.values().forEach(ConsumerGroup::rebalancing);
    }

    public boolean removePartition(String id) {
        Map<String, ConsumerGroup<T>> groupsSnapshot;
        synchronized (this) {
            if (partitions.remove(id) == null) {
                return false;
            }
            groupsSnapshot = new LinkedHashMap<>(groups);
        }
        groupsSnapshot.values().forEach(ConsumerGroup::rebalancing);
        return true;
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
    public synchronized ConsumerGroup<T> createConsumerGroup(String id, String rebalancing) {
        if (groups.containsKey(id)) {
            throw new IllegalArgumentException("ConsumerGroup already exists. id: " + id);
        }
        ConsumerGroup<T> group = new ConsumerGroup<>(id, rebalancing);
        group.setTopic(this);
        groups.put(id, group);
        return group;
    }

    /**
     * Retrieves a partition by its ID from this topic.
     *
     * @param id The ID of the partition to retrieve.
     * @return The partition if it exists, or null if no such partition exists.
     */
    public synchronized Partition<T> getPartition(String id) {
        return partitions.get(id);
    }

    public boolean removeConsumerGroup(String id) {
        ConsumerGroup<T> removedGroup;
        synchronized (this) {
            removedGroup = groups.remove(id);
        }
        if (removedGroup == null) {
            return false;
        }
        // Rebalancing acquires a group lock before reading its topic. Do not
        // acquire the same locks in the opposite order while detaching a group.
        removedGroup.detachFromTopic(this);
        return true;
    }

    /**
     * Provides a string representation of the topic, including its partitions and
     * their events.
     *
     * @param indent The level of indentation to use for formatting the output.
     * @return A string with the topic's information, formatted with indentation.
     */
    @Override
    public synchronized String getInfo(int indent) {
        String pre = " ".repeat(indent * INDENT);
        StringBuilder sb = new StringBuilder();
        sb.append(pre).append("Topic id: ").append(id).append("\n");
        partitions.values().stream().forEach(p -> sb.append(p.getInfo(indent + 1)));
        return sb.toString();
    }
}
