package tributary.core.reblancing;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import tributary.api.Consumer;
import tributary.api.Partition;
import tributary.api.Topic;

public class RangeStrategy implements RebalancingStrategy {

    @Override
    public <T> void rebalancing(Topic<T> topic, Map<String, Consumer<T>> consumers) {
        Map<String, Partition<T>> partitions = topic.getPartitions();
        int partitionNum = partitions.size();
        int consumerNum = consumers.size();
        if (partitionNum == 0 || consumerNum == 0) {
            return;
        }
        int countPerConsumer = partitionNum / consumerNum;
        int remainingPartition = partitionNum % consumerNum;

        List<Partition<T>> partitionList = new LinkedList<>(partitions.values());

        for (Consumer<T> consumer : consumers.values()) {
            List<Partition<T>> list = new LinkedList<>();
            for (int i = 0; i < countPerConsumer; i++) {
                list.add(partitionList.remove(0));
            }
            if (remainingPartition > 0) {
                list.add(partitionList.remove(0));
                remainingPartition--;
            }
            consumer.setPartitions(list);
        }
    }

    @Override
    public String getName() {
        return "Range";
    }
}
