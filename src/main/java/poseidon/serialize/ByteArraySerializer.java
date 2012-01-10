package poseidon.serialize;

import org.apache.hadoop.hbase.util.Bytes;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;


/**
 * @author Ashwin
 */


public class ByteArraySerializer {

    private static enum TYPES {
        INTEGER(1),
        STRING(2),
        DOUBLE(3),
        FLOAT(4);

        private int prefixes;
        private byte byteRepresentation;

        TYPES(int prefixes) {
            this.prefixes = prefixes;
            this.byteRepresentation = (byte) prefixes;
        }

        protected byte byteRepresentation(){
            return this.byteRepresentation;
        }

        protected byte[] internalRepresentation() {
            byte[] bytes = new byte[1];
            bytes[0] =(byte) prefixes;
            return bytes;
        }
    }

    private static Object toSerialize;
    private static byte[] toDeSerialize;

    private static List<Method> serializingMethods = new ArrayList<Method>();
    private static List<Method> deSerializingMethods = new ArrayList<Method>();

    static {
        Method[] methods = ByteArraySerializer.class.getDeclaredMethods();
        for (Method method : methods) {
            if (method.getName().startsWith("serialize"))
                serializingMethods.add(method);
            if (method.getName().startsWith("deSerialize"))
                deSerializingMethods.add(method);
        }
    }

    private static byte[] serializeInt() {
        if (toSerialize instanceof Integer) {
            byte[] bytes = Bytes.toBytes((Integer) toSerialize);
            return Bytes.add(TYPES.INTEGER.internalRepresentation(), bytes);
        }
        return null;
    }

    private static byte[] serializeString() {
        if (toSerialize instanceof String) {
            byte[] bytes = Bytes.toBytes((String) toSerialize);
            return Bytes.add(TYPES.STRING.internalRepresentation(), bytes);
        }
        return null;
    }

     private static byte[] serializeDouble() {
        if (toSerialize instanceof Double) {
            byte[] bytes = Bytes.toBytes((Double) toSerialize);
            return Bytes.add(TYPES.DOUBLE.internalRepresentation(), bytes);
        }
        return null;
    }

    private static byte[] serializeFloat() {
        if (toSerialize instanceof Float) {
            byte[] bytes = Bytes.toBytes((Float) toSerialize);
            return Bytes.add(TYPES.FLOAT.internalRepresentation(), bytes);
        }
        return null;
    }

    private static Object deSerializeInt() {
        byte[] bytes = Bytes.tail(toDeSerialize, toDeSerialize.length - 1);
        if(toDeSerialize[0] == TYPES.INTEGER.byteRepresentation())
            return Bytes.toInt(bytes);
        return null;
    }

    private static Object deSerializeString() {
        byte[] bytes = Bytes.tail(toDeSerialize, toDeSerialize.length - 1);
        if(toDeSerialize[0] == TYPES.STRING.byteRepresentation())
            return Bytes.toString(bytes);
        return null;
    }


    private static Object deSerializeDouble() {
        byte[] bytes = Bytes.tail(toDeSerialize, toDeSerialize.length - 1);
        if(toDeSerialize[0] == TYPES.DOUBLE.byteRepresentation())
            return Bytes.toDouble(bytes);
        return null;
    }

    private static Object deSerializeFloat() {
        byte[] bytes = Bytes.tail(toDeSerialize, toDeSerialize.length - 1);
        if(toDeSerialize[0] == TYPES.FLOAT.byteRepresentation())
            return Bytes.toFloat(bytes);
        return null;
    }


    public static byte[] fromObject(final Object data) {

        toSerialize = data;

        for (Method method : serializingMethods) {
            Object toReturn = null;
            try {
                toReturn = method.invoke(null);
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            }
            if (toReturn != null)
                return (byte[]) toReturn;
        }
        return null;
    }


    public static Object toObject(final byte[] data) {

        toDeSerialize = data;

        for (Method method : deSerializingMethods) {
            Object toReturn = null;
            try {
                toReturn = method.invoke(null);
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            }
            if (toReturn != null)
                return toReturn;
        }
        return null;
    }


}
