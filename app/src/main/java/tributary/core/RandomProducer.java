package tributary.core;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import tributary.api.Event;
import tributary.api.Partition;
import tributary.api.Producer;
import tributary.api.Topic;

public class RandomProducer<T> extends Producer<T> {
    private Random random;

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
        List<Partition<T>> partitiions = new LinkedList<>(topic.getPartitions().values());
        if (partitiions.isEmpty()) {
            throw new IllegalArgumentException("Topic does not have partitions.");
        }
        int index = random.nextInt(partitiions.size());
        Partition<T> partition = partitiions.get(index);
        partition.pushEvent(event);
        return partition.getId();
    }

}
