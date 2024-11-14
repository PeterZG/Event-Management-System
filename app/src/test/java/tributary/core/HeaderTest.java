package tributary.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import java.time.LocalDateTime;
import org.junit.jupiter.api.Test;

public class HeaderTest {
    @Test
    public void constructorTest() {
        Header header = new Header("h1");
        assertEquals("h1", header.getId());
        assertEquals("String", header.getPlayloadType());
        Header header2 = new Header();
        assertEquals("String", header2.getPlayloadType());
        Header header3 = new Header("h3", null, null);
        assertEquals("String", header3.getPlayloadType());
    }

    @Test
    public void constructorTest2() {
        LocalDateTime time = LocalDateTime.now();

        Header header = new Header("h1", time, "String");
        assertEquals("h1", header.getId());
        assertEquals(time, header.getCreated());
        assertEquals("String", header.getPlayloadType());
    }
}
