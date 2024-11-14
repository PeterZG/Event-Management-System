package tributary.cli;

import java.util.Scanner;

public class TributaryCLI {
    private Scanner scanner;
    private TributarySystem system;

    public static void main(String[] args) {
        TributaryCLI cli = new TributaryCLI();
        cli.start();
    }

    public TributaryCLI() {
        scanner = new Scanner(System.in);
        system = TributarySystem.getInstance();
    }

    private void start() {
        System.out.println("==========  Tributry System  ==========");
        System.out.println("Enter h/help for help.");
        while (true) {
            System.out.println("\nEnter your commnd:");
            String line = scanner.nextLine().trim();

            try {
                if (line.equals("exit")) {
                    System.out.println("Goodbye.");
                    return;
                }

                String[] strs = line.split("\\s+");
                if (specialCommands(strs)) {
                    continue;
                }
                switch (strs[0]) {
                    case "create":
                        createCommands(strs);
                        break;
                    case "delete":
                        deleteCommands(strs);
                        break;
                    case "produce":
                        produceCommands(strs);
                        break;
                    case "consume":
                        consumeCommands(strs);
                        break;
                    case "show":
                        showCommands(strs);
                        break;
                    case "parallel":
                        parallelCommands(strs, line);
                        break;
                    case "set":
                        setCommands(strs);
                        break;
                    case "playback":
                        playbackCommands(strs);
                        break;
                    default:
                        System.out.println("Invalid command.");
                }
            } catch (IllegalArgumentException e) {
                System.err.println("Invlid commnd: " + line);
                System.err.println(e.getMessage() == null ? "" : e.getMessage());
            } catch (Exception e) {
                e.printStackTrace();
                System.err.println("Invlid commnd: " + line);
            }
        }
    }

    private boolean specialCommands(String[] strs) {
        if (strs[0].equals("h") || strs[0].equals("help")) {
            printHelp();
            return true;
        }
        if (strs.length < 3) {
            System.out.println("Invalid command. Enter h/help for help.");
            return true;
        }
        return false;
    }

    private void printHelp() {
        String[] helps = new String[] {
                "help: print this help menu.",
                "exit: Exit the Tributry System.",
                "",
                "Other commands:",
                "create topic <id> <type>",
                "create partition <topic> <id>",
                "create consumer group <id> <topic> <rebalancing>",
                "create consumer group <id> <topic> <rebalancing>",
                "delete consumer <consumer>",
                "create producer <id> <type> <allocation>",
                "delete producer <producer>",
                "produce event <producer> <topic> <event> <partition>",
                "consume event <consumer> <partition>",
                "consume events <consumer> <partition> <number of events>",
                "show topic <topic>",
                "show consumer group <group>",
                "parallel produce (<producer>, <topic>, <event>), ...",
                "parallel consume (<consumer>, <partition>)",
                "set consumer group rebalancing <group> <rebalancing>",
                "playback <consumer> <partition> <offset>",
                ""
        };
        for (String help : helps) {
            System.out.println(help);
        }
    }

    private void checkArgCount(String[] args, int count) {
        if (args.length < count) {
            throw new IllegalArgumentException("Error argument count.");
        }
    }

    private void createCommands(String[] strs) {
        checkArgCount(strs, 4);
        switch (strs[1]) {
            case "topic":
                system.createTopic(strs[2], strs[3]);
                break;
            case "partition":
                system.createPartition(strs[2], strs[3]);
                break;
            case "consumer":
                if ("group".equals(strs[2])) {
                    checkArgCount(strs, 6);
                    system.createConsumerGroup(strs[3], strs[4], strs[5]);
                } else {
                    system.createConsumer(strs[2], strs[3]);
                }
                break;
            case "producer":
                checkArgCount(strs, 5);
                system.createProducer(strs[2], strs[3], strs[4]);
                break;
            default:
                throw new IllegalArgumentException();
        }
    }

    private void deleteCommands(String[] strs) {
        switch (strs[1]) {
            case "consumer":
                system.deleteConsumer(strs[2]);
                break;
            case "producer":
                system.deleteProducer(strs[2]);
                break;
            default:
        }
    }

    private void produceCommands(String[] strs) {
        checkArgCount(strs, 5);
        String partitionId = strs.length > 5 ? strs[5] : null;
        system.produceEvent(strs[2], strs[3], strs[4], partitionId);
    }

    private void consumeCommands(String[] strs) {
        checkArgCount(strs, 4);
        switch (strs[1]) {
            case "event":
                system.consumeEvent(strs[2], strs[3]);
                break;
            case "events":
                checkArgCount(strs, 5);
                system.consumeEvents(strs[2], strs[3], Integer.parseInt(strs[4]));
                break;
            default:
        }
    }

    private void showCommands(String[] strs) {
        switch (strs[1]) {
            case "topic":
                system.showTopic(strs[2]);
                break;
            case "consumer":
                checkArgCount(strs, 4);
                system.showConsumerGroup(strs[3]);
                break;
            default:
        }
    }

    private void parallelCommands(String[] strs, String line) {
        switch (strs[1]) {
            case "produce":
                system.parallelProduce(line);
                break;
            case "consume":
                system.parallelConsume(line);
                break;
            default:
        }
    }

    private void setCommands(String[] strs) {
        checkArgCount(strs, 6);
        if (!strs[1].equals("consumer")
                || !strs[2].equals("group")
                || !strs[3].equals("rebalancing")) {
            throw new IllegalArgumentException();
        }
        system.setConsumerGroupRebalancing(strs[4], strs[5]);
    }

    private void playbackCommands(String[] strs) {
        checkArgCount(strs, 4);
        system.playback(strs[1], strs[2], Integer.parseInt(strs[3]));
    }

}
