package tributary.core;

import java.time.LocalDateTime;
import java.util.UUID;

public class Header {

    private LocalDateTime created;
    private String id;
    private String payloadType;

    public Header() {
        created = LocalDateTime.now();
        id = UUID.randomUUID().toString();
        payloadType = "String";
    }

    public Header(String id) {
        created = LocalDateTime.now();
        this.id = id;
        payloadType = "String";
    }

    public Header(String id, LocalDateTime created, String payload) {
        this.id = id;
        this.created = created == null ? LocalDateTime.now() : created;
        this.payloadType = payload == null ? "String" : payload;
    }

    public LocalDateTime getCreated() {
        return created;
    }

    public String getId() {
        return id;
    }

    public String getPlayloadType() {
        return payloadType;
    }

}
