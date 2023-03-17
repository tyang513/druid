package com.talkingdata.olap.druid;

import java.util.function.Supplier;

/**
 * DebugLogger
 *
 * @author bingxin.li
 * @version v1
 * @since 2022-07-01
 */
public interface DebugLogger {
    void debug(String message, Object... args);

    void debug(String message, Supplier<Object> supplier);

    void info(String message, Object... args);

    void error(String message, Throwable e);
}
