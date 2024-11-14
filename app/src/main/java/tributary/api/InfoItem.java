package tributary.api;

/**
 * The InfoItem interface defines a contract for classes that can provide a
 * structured
 * string representation of their state or information.
 */
public interface InfoItem {
    /**
     * The INDENT defines the number of spaces to use for each level of indentation
     */
    static int INDENT = 2;

    /**
     * Returns a formatted string that represents the information of the
     * implementing item. The string should be indented according to the specified
     * level of indentation.
     *
     * @param indent The indentation level
     * @return A string representation of the item's information
     */
    String getInfo(int indent);

}
