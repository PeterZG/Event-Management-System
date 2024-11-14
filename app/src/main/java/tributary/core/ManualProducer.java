package tributary.core;

import tributary.api.Event;
import tributary.api.Partition;
import tributary.api.Producer;
import tributary.api.Topic;

public class ManualProducer<T> extends Producer<T> {

    public ManualProducer(String id) {
        super(id);
    }

    @Override
    public Event<T> produce(String id, String key, T value) {
        Header header = new Header(id);
        if (key == null) {
            throw new IllegalArgumentException("Key is needed.");
        }
        return new Event<>(header, key, value);
    }

    @Override
    public String allocate(Event<T> event, Topic<T> topic) {
        if (event.getKey() == null || topic.getPartition(event.getKey()) == null) {
            throw new IllegalArgumentException("Can not allocate event to partition id: " + event.getKey());
        }
        Partition<T> partition = topic.getPartition(event.getKey());
        partition.pushEvent(event);
        return partition.getId();
    }

}
