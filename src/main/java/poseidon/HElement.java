package poseidon;

import com.tinkerpop.blueprints.pgm.Element;
import poseidon.util.LogUtil;

import java.util.Arrays;
import java.util.logging.Logger;

/**
 * @author Ashwin
 */
public abstract class HElement implements Element {

    protected Logger LOG = LogUtil.getLoggerFor(HElement.class);

    protected HGraph hGraph;
    protected byte[] id;

    public void setId(byte[] id) {
        this.id = id;
    }

    @Override
    public Object getId() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof HElement)) return false;

        HElement hElement = (HElement) o;

        if (!Arrays.equals(id, hElement.id)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return id != null ? Arrays.hashCode(id) : 0;
    }
}
