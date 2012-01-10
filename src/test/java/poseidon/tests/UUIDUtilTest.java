package poseidon.tests;

import poseidon.util.HBaseUtils;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.Is.is;

/**
 * @author Ashwin
 */
public class UUIDUtilTest {

    @Test
    public void shouldGenerateUUIDAsByteArray(){
        byte[] id = HBaseUtils.newIdentifier();
        byte[] id2 = HBaseUtils.newIdentifier();
        assertThat(id.length,is(16));
        assertThat(id2.length,is(16));
        assertThat(id, not(id2));

    }

}
