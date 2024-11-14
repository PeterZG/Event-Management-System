package tributary.api;

import tributary.core.ManualProducer;
import tributary.core.RandomProducer;

/**
 * The ProducerFactory class serves as a factory for creating instances of
 * producers with different allocation strategies.
 *
 * @param <T> The type of payload the produced events will carry.
 */
public class ProducerFactory<T> {
    /**
     * Creates a new producer with the specified ID and allocation strategy.
     *
     * @param id         The unique identifier for the producer.
     * @param allocation The allocation strategy for the producer, which can be
     *                   "random" or "manual".
     * @return A new instance of a producer with the specified allocation strategy.
     * @throws IllegalArgumentException If the allocation strategy is not supported.
     */
    public Producer<T> create(String id, String allocation) {
        String type = String.valueOf(allocation).toLowerCase();
        switch (type) {
            case "random":
                return new RandomProducer<>(id);
            case "manual":
                return new ManualProducer<>(id);
            default:
                throw new IllegalArgumentException("Not supported allocation type: " + allocation);
        }
    }
}
