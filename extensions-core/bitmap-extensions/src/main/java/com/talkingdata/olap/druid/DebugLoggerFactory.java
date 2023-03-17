package com.talkingdata.olap.druid;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DebugLoggerFactory
 *
 * @author bingxin.li
 * @version v1
 * @since 2022-07-08
 */
public class DebugLoggerFactory {
    public static DebugLogger getLogger(Class clazz) {
        Logger logger = LoggerFactory.getLogger(clazz);
        return logger.isDebugEnabled() ? new DefaultLogger(logger) : new NothingLogger(logger);
    }

    private static class DefaultLogger implements DebugLogger {
        private static final AtomicLong INDEX = new AtomicLong();
        private final Logger logger;

        public DefaultLogger(Logger logger) {
            this.logger = logger;
        }

        public void debug(String message, Object... args) {
            StringBuilder sb = toStringBuilder(message);

            if (args != null) {
                for (int i = 0; i < args.length; i++) {
                    if (i > 0) {
                        sb.append(", ");
                    }
                    sb.append("arg").append(i).append("[").append(toLog(args[i])).append("]");
                }
            }

            logger.debug(sb.toString());
        }

        private StringBuilder toStringBuilder(String message) {
            return new StringBuilder(256)
                    .append("【")
                    .append(INDEX.incrementAndGet())
                    .append("】")
                    .append(message)
                    .append(": ");
        }

        public void debug(String message, Supplier<Object> supplier) {
            StringBuilder sb = toStringBuilder(message);

            if (supplier != null) {
                sb.append(toLog(supplier.get()));
            }

            logger.debug(sb.toString());
        }

        public void info(String message, Object... args) {
            StringBuilder sb = toStringBuilder(message);

            if (args != null) {
                for (int i = 0; i < args.length; i++) {
                    if (i > 0) {
                        sb.append(", ");
                    }
                    sb.append("arg").append(i).append("[").append(toLog(args[i])).append("]");
                }
            }

            logger.info(sb.toString());
        }

        private String toLog(Object value) {
            if (value == null) {
                return "null";
            } else if (value instanceof List) {
                return new StringBuilder(128)
                        .append("class=")
                        .append(value.getClass().getSimpleName())
                        .append(", hash=")
                        .append(value.hashCode())
                        .append(", value=")
                        .append(StringUtils.join((List) value))
                        .toString();
            } else {
                return "class=" + value.getClass().getSimpleName() + ", hash=" + value.hashCode() + ", value=" + value;
            }
        }

        @Override
        public void error(String message, Throwable e) {
            logger.error(message, e);
        }
    }

    private static class NothingLogger implements DebugLogger {

        private final Logger logger;

        public NothingLogger(Logger logger) {
            this.logger = logger;
        }

        @Override
        public void debug(String message, Object... args) {
        }

        @Override
        public void debug(String message, Supplier<Object> supplier) {
        }

        @Override
        public void info(String message, Object... args) {

        }

        @Override
        public void error(String message, Throwable e) {
            logger.error(message, e);
        }
    }
}
