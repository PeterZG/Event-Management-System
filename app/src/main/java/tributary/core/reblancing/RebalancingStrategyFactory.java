package tributary.core.reblancing;

public class RebalancingStrategyFactory {
    public RebalancingStrategy create(String type) {
        String lowType = String.valueOf(type).toLowerCase();
        switch (lowType) {
            case "range":
                return new RangeStrategy();
            case "roundrobin":
                return new RoundRobinStrategy();
            default:
                throw new IllegalArgumentException("Not supported strategy type: " + type);
        }
    }
}
