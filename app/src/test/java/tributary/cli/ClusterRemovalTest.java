package tributary.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import tributary.api.Consumer;
import tributary.api.ConsumerGroup;
import tributary.api.Event;
import tributary.api.Topic;

public class ClusterRemovalTest {
    private static class RecordingConsumer extends Consumer<String> {
        RecordingConsumer(String id) {
            super(id);
        }

        @Override
        public void consume(Event<String> event) {
        }
    }

    @Test
    public void deletingTopicDetachesConsumersButPreservesOtherTopics() {
        Cluster cluster = new Cluster();
        cluster.addTopic(new Topic<String>("removed-topic"));
        cluster.addTopic(new Topic<String>("retained-topic"));
        cluster.createPartition("removed-topic", "removed-partition");
        cluster.createPartition("retained-topic", "retained-partition");
        ConsumerGroup<String> removedGroup = cluster.createConsumerGroup("removed-topic", "removed-group", "range");
        ConsumerGroup<String> retainedGroup = cluster.createConsumerGroup("retained-topic", "retained-group", "range");
        Consumer<String> removedConsumer = new RecordingConsumer("removed-consumer");
        Consumer<String> retainedConsumer = new RecordingConsumer("retained-consumer");
        removedGroup.addConsumer(removedConsumer);
        retainedGroup.addConsumer(retainedConsumer);

        cluster.removeTopic("removed-topic");

        assertNull(cluster.getTopic("removed-topic"));
        assertNull(cluster.getConsumerGroup("removed-group"));
        assertTrue(removedConsumer.getPartitions().isEmpty());
        assertEquals(1, retainedConsumer.getPartitions().size());
        assertEquals(retainedGroup, cluster.getConsumerGroup("retained-group"));
    }
}
