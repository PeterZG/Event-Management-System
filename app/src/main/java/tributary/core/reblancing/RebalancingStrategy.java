package tributary.core.reblancing;

import java.util.Map;

import tributary.api.Consumer;
import tributary.api.Topic;

public interface RebalancingStrategy {
    <T> void rebalancing(Topic<T> topic, Map<String, Consumer<T>> consumers);

    String getName();
}
