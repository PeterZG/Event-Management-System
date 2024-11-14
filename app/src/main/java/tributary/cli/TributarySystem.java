package tributary.cli;

import java.io.File;
import java.lang.reflect.Constructor;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.JSONObject;

import tributary.api.Consumer;
import tributary.api.ConsumerGroup;
import tributary.api.Event;
import tributary.api.Partition;
import tributary.api.Producer;
import tributary.api.ProducerFactory;
import tributary.api.Topic;

public class TributarySystem {
    private Cluster cluster;

    private static TributarySystem instance;

    private TributarySystem() {
        cluster = new Cluster();
    }

    public static TributarySystem getInstance() {
        if (instance == null) {
            instance = new TributarySystem();
        }
        return instance;
    }

    private void checkType(String type) {
        if (!(type.equalsIgnoreCase("Integer") || type.equalsIgnoreCase("String"))) {
            throw new IllegalArgumentException("Not supported type: " + type);
        }
    }

    private void checkFileExists(String filename) {
        File file = new File(filename);
        if (!file.exists()) {
            throw new IllegalArgumentException("File does not exist. " + filename);
        }
    }

    public void createTopic(String id, String type) {
        checkType(type);
        if (cluster.getTopic(id) != null) {
            throw new IllegalArgumentException("Topic with id '" + id + "' already exists.");
        }

        if (type.equalsIgnoreCase("String")) {
            cluster.addTopic(new Topic<String>(id));
        } else if (type.equalsIgnoreCase("Integer")) {
            cluster.addTopic(new Topic<Integer>(id));
        }
        System.out.println("Topic '" + id + "' for type '" + type + "' created.");
    }

    public void createPartition(String topicId, String partitionId) {
        cluster.checkTopic(topicId);
        cluster.createPartition(topicId, partitionId);
        System.out.println("Partition created. id: " + partitionId);
    }

    public void createConsumerGroup(String groupId, String topicId, String rebalancing) {
        cluster.createConsumerGroup(topicId, groupId, rebalancing);
        System.out.println("Consumer group created. id: " + groupId);
    }

    public void createConsumer(String groupId, String consumerId) {
        ConsumerGroup group = cluster.getConsumerGroup(groupId);
        if (group == null) {
            throw new IllegalArgumentException("Consumer group does not exist. id: " + groupId);
        }
        if (group.getConsumer(consumerId) != null) {
            throw new IllegalArgumentException("Consumer exists. id: " + consumerId);
        }
        Constructor<BasicConsumer> c;
        try {
            c = BasicConsumer.class.getConstructor(String.class);
            group.createConsumer(consumerId, c);
            System.out.println("Consumer created. id: " + consumerId);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void deleteConsumer(String id) {
        ConsumerGroup group = cluster.getGroupOfConsumer(id);
        if (group == null) {
            throw new IllegalArgumentException("Consumer does not exist. id: " + id);
        } else {
            group.removeConsumer(id);
            System.out.println("Consumer removed. id: " + id);
            System.out.println("Consumer group rebalanced. id: " + group.getId());
        }
    }

    public void createProducer(String id, String type, String allocation) {
        checkType(type);
        if (cluster.getProducer(id) != null) {
            throw new IllegalArgumentException("Producer already exists. id: " + id);
        }

        if (type.equalsIgnoreCase("String")) {
            ProducerFactory<String> factory = new ProducerFactory<>();
            cluster.addProducer(factory.create(id, allocation));
        } else if (type.equalsIgnoreCase("Integer")) {
            ProducerFactory<Integer> factory = new ProducerFactory<>();
            cluster.addProducer(factory.create(id, allocation));
        }
        System.out.println("Producer created. id: " + id);
    }

    public void deleteProducer(String id) {
        Producer producer = cluster.removeProducer(id);
        if (producer == null) {
            throw new IllegalArgumentException("Producer does not exist. id: " + id);
        } else {
            System.out.println("Producer removed. id: " + id);
        }
    }

    public void produceEvent(String producerId, String topicId, String eventFile, String partitionId) {
        Producer producer = cluster.checkProducer(producerId);
        Topic topic = cluster.checkTopic(topicId);
        checkFileExists(eventFile);
        if (partitionId != null && topic.getPartition(partitionId) == null) {
            throw new IllegalArgumentException("Partition does not exist. id: " + partitionId);
        }

        JSONObject eventJson = null;
        String eventId = null;
        Object value = null;
        try {
            eventJson = new JSONObject(new String(Files.readAllBytes(Paths.get(eventFile))));
            eventId = eventJson.getString("id");
            value = eventJson.getString("value");
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalArgumentException("Error parse json data.");
        }

        Event event = producer.produce(eventId, partitionId, value);
        String allocated = producer.allocate(event, topic);

        System.out.println("Event (id " + event.getId() + ") allocated to partition (id: " + allocated + ")");
    }

    public void consumeEvent(String consumerId, String partitionId) {
        Consumer consumer = cluster.checkConsumer(consumerId);
        Partition partition = cluster.checkPartition(partitionId);
        if (partition.size() > 0) {
            Event event = partition.popEvent();
            consumer.consume(event);
        } else {
            throw new IllegalArgumentException("Partition is empty. id: " + partitionId);
        }
    }

    public void consumeEvents(String consumerId, String partitionId, int number) {
        Consumer consumer = cluster.checkConsumer(consumerId);
        Partition partition = cluster.checkPartition(partitionId);
        while (number > 0 && partition.size() > 0) {

            Event event = partition.popEvent();
            consumer.consume(event);
            number--;
        }
    }

    public void showTopic(String id) {
        Topic topic = cluster.checkTopic(id);
        System.out.println(topic.getInfo(0));
    }

    public void showConsumerGroup(String id) {
        ConsumerGroup group = cluster.getConsumerGroup(id);
        if (group == null) {
            throw new IllegalArgumentException("Consumer group does not exist. id: " + id);
        }
        System.out.println(group.getInfo(0));
    }

    public void parallelProduce(String command) {
        Pattern pattern = Pattern.compile("^.*?((\\(.*?,.*?,.*?\\))(\\s*,\\s*)?)+$");
        Pattern paramPattern = Pattern.compile("\\(.*?,.*?,.*?\\)");
        List<String[]> params = parseParams(command, pattern, paramPattern);
        for (String[] strs : params) {
            cluster.checkProducer(strs[0]);
            cluster.checkTopic(strs[1]);
            checkFileExists(strs[2]);
        }
        List<Thread> threads = new ArrayList<>();
        for (String[] strs : params) {
            Thread thread = new Thread(() -> {
                produceEvent(strs[0], strs[1], strs[2], null);
            });
            threads.add(thread);
            thread.start();
        }
        joinThreads(threads);
    }

    public void parallelConsume(String command) {
        Pattern pattern = Pattern.compile("^.*?((\\(.*?,.*?\\))(\\s*,\\s*)?)+$");
        Pattern paramPattern = Pattern.compile("\\(.*?,.*?\\)");
        List<String[]> params = parseParams(command, pattern, paramPattern);
        for (String[] strs : params) {
            cluster.checkConsumer(strs[0]);
            cluster.checkPartition(strs[1]);
        }
        List<Thread> threads = new ArrayList<>();
        for (String[] strs : params) {
            Thread thread = new Thread(() -> {
                consumeEvent(strs[0], strs[1]);
            });
            threads.add(thread);
            thread.start();
        }
        joinThreads(threads);

    }

    private List<String[]> parseParams(String command, Pattern pattern, Pattern paramPattern) {
        if (!pattern.matcher(command).find()) {
            throw new IllegalArgumentException();
        }
        List<String[]> params = new ArrayList<>();
        Matcher paramMatcher = paramPattern.matcher(command);
        while (paramMatcher.find()) {
            String str = paramMatcher.group();
            str = str.trim().substring(1, str.length() - 1);
            params.add(str.replace(" ", "").split(","));
        }
        return params;
    }

    private void joinThreads(List<Thread> threads) {
        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void setConsumerGroupRebalancing(String groupId, String rebalancing) {
        ConsumerGroup group = cluster.getConsumerGroup(groupId);
        if (group == null) {
            throw new IllegalArgumentException("Consumer group does not exist. id: " + groupId);
        }
        group.setRebalancing(rebalancing);
        System.out.println("Consumer group set rebalancing strategy to " + rebalancing);
    }

    public void playback(String consumerId, String partitionId, int offset) {
        Consumer consumer = cluster.checkConsumer(consumerId);
        Partition partition = cluster.checkPartition(partitionId);
        List<Event> played = partition.getPlayedEvents();
        if (offset > played.size()) {
            throw new IllegalArgumentException("Invalid offset, max played offset is " + played.size());
        }
        for (int i = offset - 1; i < played.size(); i++) {
            consumer.consume(played.get(i));
        }
    }

}
