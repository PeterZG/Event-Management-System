package tributary.api;

import java.lang.reflect.Constructor;
import java.util.LinkedHashMap;
import java.util.Map;

import tributary.core.reblancing.RebalancingStrategy;
import tributary.core.reblancing.RebalancingStrategyFactory;

/**
 * The ConsumerGroup class represents a collection of consumers that are
 * assigned to consume events from a topic's partitions.
 *
 * @param <T> The type of payload of the events.
 */
public class ConsumerGroup<T> implements InfoItem {
    /**
     * The identifier for this consumer group.
     */
    private String id;
    /**
     * The topic that this consumer group is subscribed to.
     */
    private Topic<T> topic;
    /**
     * A map of consumers within this group.
     */
    private Map<String, Consumer<T>> consumers = new LinkedHashMap<>();
    /**
     * The rebalancing strategy used by this group to distribute partitions among
     * consumers.
     */
    private RebalancingStrategy rebalancingStrategy;
    /**
     * The factory used to create rebalancing strategies.
     */
    private RebalancingStrategyFactory factory;

    /**
     * Constructs a new ConsumerGroup with the specified ID and rebalancing
     * strategy.
     *
     * @param id                  The unique identifier for the consumer group.
     * @param rebalancingStrategy The initial rebalancing strategy for the consumer
     *                            group.
     */
    public ConsumerGroup(String id, String rebalancingStrategy) {
        factory = new RebalancingStrategyFactory();
        this.id = id;
        this.rebalancingStrategy = factory.create(rebalancingStrategy);
    }

    /**
     * Retrieves the identifier of this consumer group.
     *
     * @return The ID of the consumer group.
     */
    public String getId() {
        return id;
    }

    /**
     * Initiates the rebalancing process, redistributing partitions among the
     * consumers in the group.
     */
    public void rebalancing() {
        rebalancingStrategy.rebalancing(topic, consumers);
    }

    /**
     * Adds a consumer to the consumer group.
     *
     * @param consumer The consumer to be added.
     */
    public void addConsumer(Consumer<T> consumer) {
        consumers.put(consumer.getId(), consumer);
        rebalancing();
    }

    /**
     * Creates a new consumer using the provided constructor and adds it to the
     * group.
     *
     * @param id          The unique identifier for the new consumer.
     * @param constructor The constructor used to instantiate the new consumer.
     * @throws IllegalArgumentException If the constructor is invalid or an
     *                                  exception occurs during instantiation.
     */
    public void createConsumer(String id, Constructor<? extends Consumer<T>> constructor) {
        try {
            Consumer<T> consumer = constructor.newInstance(id);
            addConsumer(consumer);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid  constructor. ");
        }
    }

    /**
     * Removes a consumer from the consumer group by its ID.
     *
     * @param id The ID of the consumer to remove.
     * @return true if the consumer was successfully removed; false otherwise.
     */
    public boolean removeConsumer(String id) {
        if (consumers.remove(id) != null) {
            rebalancing();
            return true;
        }
        return false;
    }

    /**
     * Retrieves a consumer from the group by its ID.
     *
     * @param id The ID of the consumer to retrieve.
     * @return The consumer if it exists in the group; null otherwise.
     */
    public Consumer<T> getConsumer(String id) {
        return consumers.get(id);
    }

    /**
     * Sets the rebalancing strategy for the consumer group.
     *
     * @param rebalancing The new rebalancing strategy.
     */
    public void setRebalancing(String rebalancing) {
        this.rebalancingStrategy = factory.create(rebalancing);
        rebalancing();
    }

    /**
     * Associates the consumer group with a topic.
     *
     * @param topic The topic to associate with this consumer group.
     */
    public void setTopic(Topic<T> topic) {
        this.topic = topic;
    }

    /**
     * Provides a string representation of the consumer group.
     *
     * @param indent The level of indentation to use for formatting the output.
     * @return A string with the consumer group's information
     */
    @Override
    public String getInfo(int indent) {
        String pre = " ".repeat(indent * INDENT);
        StringBuilder sb = new StringBuilder();
        sb.append(pre).append("Consumer group id: ").append(id).append(", rebalancing: ")
                .append(rebalancingStrategy.getName()).append("\n");
        consumers.values().stream().forEach(c -> sb.append(c.getInfo(indent + 1)));
        return sb.toString();
    }
}
