package tributary.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import tributary.api.Event;
import tributary.api.Partition;
import tributary.api.Producer;
import tributary.api.Topic;

public class RandomProducer<T> extends Producer<T> {
    private final Random random;

    public RandomProducer(String id) {
        super(id);
        random = new Random();
    }

    @Override
    public Event<T> produce(String id, String key, T value) {
        Header header = new Header(id);
        return new Event<>(header, null, value);
    }

    @Override
    public String allocate(Event<T> event, Topic<T> topic) {
        List<Partition<T>> availablePartitions = new ArrayList<>(topic.getPartitions().values());
        if (availablePartitions.isEmpty()) {
            throw new IllegalArgumentException("Topic does not have partitions.");
        }
        int index = random.nextInt(availablePartitions.size());
        Partition<T> partition = availablePartitions.get(index);
        partition.pushEvent(event);
        return partition.getId();
    }

}
