package poseidon.util;

import java.util.logging.Logger;

/**
 * @author ashwin
 */
public class LogUtil {

    public static Logger getLoggerFor(Class clazz){
        return Logger.getLogger(clazz.getName());
    }

}
