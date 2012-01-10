package poseidon.tests;

import poseidon.serialize.ByteArraySerializer;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

/**
 * @author Ashwin
 */
public class ByteArraySerializerTest {


    @Test
    public void shouldSerializeStringTypedObject() {
        String data = new String("ashwin");
        byte[] bytes = ByteArraySerializer.fromObject(data);
        byte[] rep = new byte[1];
        rep[0] = 2;
        assertThat(bytes, is(Bytes.add(rep, Bytes.toBytes("ashwin"))));
    }


    @Test
    public void shouldSerializeIntegerTypedObject() {
        int data = 10;
        byte[] bytes = ByteArraySerializer.fromObject(data);
        byte[] rep = new byte[1];
        rep[0] = 1;
        assertThat(bytes, is(Bytes.add(rep, Bytes.toBytes(10))));
    }

    @Test
      public void shouldSerializeDoubleTypedObject() {
          double data = 5.5;
          byte[] bytes = ByteArraySerializer.fromObject(data);
          byte[] rep = new byte[1];
          rep[0] = 3;
          assertThat(bytes, is(Bytes.add(rep, Bytes.toBytes(5.5))));
      }


      @Test
      public void shouldDeSerializeDoubleByteStreams() {
          double data = 5.5;
          byte[] bytes = ByteArraySerializer.fromObject(data);
          Object desData = ByteArraySerializer.toObject(bytes);
          assertThat((Double) desData, is(5.5));

      }
    


    @Test
    public void shouldDeSerializeStringByteStreams() {
        String data = new String("ashwin");
        byte[] bytes = ByteArraySerializer.fromObject(data);
        Object desData = ByteArraySerializer.toObject(bytes);
        assertThat((String) desData, is("ashwin"));

    }


     @Test
    public void shouldDeSerializeIntByteStreams() {
        int data = 10;
        byte[] bytes = ByteArraySerializer.fromObject(data);
        Object desData = ByteArraySerializer.toObject(bytes);
        assertThat((Integer) desData, is(10));

    }


}
