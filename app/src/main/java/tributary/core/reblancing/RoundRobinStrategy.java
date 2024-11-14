package tributary.core.reblancing;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import tributary.api.Consumer;
import tributary.api.Partition;
import tributary.api.Topic;

public class RoundRobinStrategy implements RebalancingStrategy {

    @Override
    public <T> void rebalancing(Topic<T> topic, Map<String, Consumer<T>> consumers) {
        Map<String, Partition<T>> partitions = topic.getPartitions();
        if (partitions.isEmpty() || consumers.isEmpty()) {
            return;
        }

        List<Partition<T>> partitionList = new LinkedList<>(partitions.values());
        consumers.values().stream().forEach(c -> c.setPartitions(new LinkedList<>()));

        while (!partitionList.isEmpty()) {
            for (Consumer<T> consumer : consumers.values()) {
                if (!partitionList.isEmpty()) {
                    consumer.addPartition(partitionList.remove(0));
                } else {
                    break;
                }
            }
        }
    }

    @Override
    public String getName() {
        return "RoundRobin";
    }

}
