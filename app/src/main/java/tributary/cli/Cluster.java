package tributary.cli;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import tributary.api.Consumer;
import tributary.api.ConsumerGroup;
import tributary.api.Partition;
import tributary.api.Producer;
import tributary.api.Topic;

public class Cluster {

    private final Map<String, Topic> topicMap = new HashMap<>();
    private final Map<String, Producer> producerMap = new HashMap<>();
    private final Map<String, ConsumerGroup> consumerGroupMap = new HashMap<>();
    private final Map<String, String> groupTopics = new HashMap<>();

    public void addTopic(Topic topic) {
        topicMap.put(topic.getId(), topic);
    }

    public Topic getTopic(String id) {
        return topicMap.get(id);
    }

    public Topic removeTopic(String id) {
        if (!topicMap.containsKey(id)) {
            return null;
        }
        for (String groupId : new ArrayList<>(groupTopics.keySet())) {
            if (id.equals(groupTopics.get(groupId))) {
                removeConsumerGroup(groupId);
            }
        }
        return topicMap.remove(id);
    }

    public void createPartition(String topicId, String partitionId) {
        if (topicMap.values().stream().anyMatch(topic -> topic.getPartition(partitionId) != null)) {
            throw new IllegalArgumentException("Partition ID already exists in the cluster: " + partitionId);
        }
        Topic topic = getTopic(topicId);
        if (topic == null) {
            throw new IllegalArgumentException("Topic does not exist. id: " + topicId);
        }
        topic.createPartition(partitionId);
    }

    public void addProducer(Producer producer) {
        producerMap.put(producer.getId(), producer);
    }

    public Producer getProducer(String id) {
        return producerMap.get(id);
    }

    public Producer removeProducer(String id) {
        return producerMap.remove(id);
    }

    public ConsumerGroup createConsumerGroup(String topicId, String groupId, String rebalancing) {
        if (consumerGroupMap.containsKey(groupId)) {
            throw new IllegalArgumentException("Consumer group already exists: " + groupId);
        }
        Topic topic = checkTopic(topicId);
        ConsumerGroup group = topic.createConsumerGroup(groupId, rebalancing);
        consumerGroupMap.put(groupId, group);
        groupTopics.put(groupId, topicId);
        return group;
    }

    public ConsumerGroup getConsumerGroup(String id) {
        return consumerGroupMap.get(id);
    }

    public boolean removeConsumerGroup(String id) {
        String topicId = groupTopics.remove(id);
        if (topicId == null) {
            return false;
        }
        checkTopic(topicId).removeConsumerGroup(id);
        consumerGroupMap.remove(id);
        return true;
    }

    public void addConsumer(String groupId, Consumer consumer) {
        ConsumerGroup group = consumerGroupMap.get(groupId);
        if (group == null) {
            throw new IllegalArgumentException("Group " + groupId + " does not exist.");
        }
        group.addConsumer(consumer);
    }

    public ConsumerGroup getGroupOfConsumer(String id) {
        Optional<ConsumerGroup> group = consumerGroupMap.values().stream().filter(g -> g.getConsumer(id) != null)
                .findFirst();
        return group.isPresent() ? group.get() : null;
    }

    public Topic checkTopic(String id) {
        Topic topic = getTopic(id);
        if (topic == null) {
            throw new IllegalArgumentException("Topic does not exist. id: " + id);
        }
        return topic;
    }

    public Producer checkProducer(String id) {
        Producer producer = getProducer(id);
        if (producer == null) {
            throw new IllegalArgumentException("Producer does not exist. id: " + id);
        }
        return producer;
    }

    public Consumer checkConsumer(String id) {
        ConsumerGroup group = getGroupOfConsumer(id);
        if (group == null) {
            throw new IllegalArgumentException("Consumer does not exist. id: " + id);
        }
        return group.getConsumer(id);
    }

    public Partition checkPartition(String id) {
        Optional<Topic> group = topicMap.values().stream().filter(g -> g.getPartition(id) != null)
                .findFirst();
        Partition partition = group.isPresent() ? group.get().getPartition(id) : null;
        if (partition == null) {
            throw new IllegalArgumentException("Partition does not exist. id: " + id);
        }
        return partition;
    }

}
