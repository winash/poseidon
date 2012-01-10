package poseidon.util;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.NavigableMap;
import java.util.UUID;


/**
 * @author Ashwin
 */
public class HBaseUtils {

    public static byte[] newIdentifier() {
        UUID uuid = UUID.randomUUID();
        ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
        bb.putLong(uuid.getMostSignificantBits());
        bb.putLong(uuid.getLeastSignificantBits());
        return bb.array();
    }

    public static byte[] getFirstValueFromMap(NavigableMap<byte[], byte[]> map) {
        Map.Entry<byte[], byte[]> entry = map.firstEntry();
        if (null == entry)
            return null;
        return entry.getValue();
    }

    public static byte[] getFirstKeyFromMap(NavigableMap<byte[], byte[]> map) {
        Map.Entry<byte[], byte[]> entry = map.firstEntry();
        if (null == entry)
            return null;
        return entry.getKey();
    }

    

}
