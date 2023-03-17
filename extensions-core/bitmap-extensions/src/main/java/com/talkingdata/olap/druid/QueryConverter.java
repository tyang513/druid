package com.talkingdata.olap.druid;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.druid.java.util.common.jackson.JacksonUtils;

/**
 * QueryConverter
 *
 * @author bingxin.li
 * @version v1
 * @since 2022-09-01
 */
public class QueryConverter {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String FIELD_INTERVALS = "intervals";
    private static final String FIELD_GRANULARITY = "granularity";
    private static final String FIELD_QUERIES = "queries";
    private static final String TIME_ZONE_OFFSET = "+08:00";
    private static final String QUERY_TIME_ZONE = "Asia/Shanghai";
    private static final Map<String, Object> GRANULARITY_RESOURCE;
    private static final QueryConverter SINGLETON = new QueryConverter();

    static {
        GRANULARITY_RESOURCE = new HashMap<>();
        GRANULARITY_RESOURCE.put("SECOND", createGranularity("PT1S"));
        GRANULARITY_RESOURCE.put("MINUTE", createGranularity("PT1M"));
        GRANULARITY_RESOURCE.put("FIVE_MINUTE", createGranularity("PT5M"));
        GRANULARITY_RESOURCE.put("TEN_MINUTE", createGranularity("PT10M"));
        GRANULARITY_RESOURCE.put("FIFTEEN_MINUTE", createGranularity("PT15M"));
        GRANULARITY_RESOURCE.put("THIRTY_MINUTE", createGranularity("PT30M"));
        GRANULARITY_RESOURCE.put("HOUR", createGranularity("PT1H"));
        GRANULARITY_RESOURCE.put("SIX_HOUR", createGranularity("PT6H"));
        GRANULARITY_RESOURCE.put("DAY", createGranularity("P1D"));
        GRANULARITY_RESOURCE.put("WEEK", createGranularity("P1W"));
        GRANULARITY_RESOURCE.put("MONTH", createGranularity("P1M"));
        GRANULARITY_RESOURCE.put("QUARTER", createGranularity("P3M"));
        GRANULARITY_RESOURCE.put("YEAR", createGranularity("P1Y"));
        GRANULARITY_RESOURCE.put("ALL", createGranularity("P500Y"));
        GRANULARITY_RESOURCE.put("NONE", createGranularity("PT1H"));
    }

    private static DateFormat createInputFormat() {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    }

    private static DateFormat createYearFormat() {
        return new SimpleDateFormat("yyyy-MM-dd");
    }

    private static DateFormat createOutputFormat() {
        return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    }

    public static QueryConverter getInstance() {
        return SINGLETON;
    }

    private QueryConverter() {
    }

    public String convert(InputStream in) throws IOException {
        Map<String, Object> map = OBJECT_MAPPER.readValue(in, JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT);
        return convertQueries(map);
    }

    public String convert(String value) throws IOException {
        Map<String, Object> map = OBJECT_MAPPER.readValue(value, JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT);
        return convertQueries(map);
    }

    private String convertQueries(Map<String, Object> map) throws IOException {
        convertQuery(map);

        Object queryItems = map.get(FIELD_QUERIES);
        if (queryItems instanceof Map) {
            Collection<Object> values = ((Map<String, Object>) queryItems).values();
            for (Object queryItem : values) {
                if (queryItem instanceof Map) {
                    convertQuery((Map<String, Object>) queryItem);
                }
            }
        }

        return OBJECT_MAPPER.writeValueAsString(map);
    }

    private void convertQuery(Map<String, Object> map) {
        Object intervals = map.get(FIELD_INTERVALS);
        if (intervals instanceof List) {
            map.put(FIELD_INTERVALS, convertIntervals((List<String>) intervals));
        }

        Object granularityValue = map.get(FIELD_GRANULARITY);
        if (granularityValue != null) {
            map.put(FIELD_GRANULARITY, convertGranularity(granularityValue));
        }
    }

    private List<String> convertIntervals(List<String> intervals) {
        return intervals.stream().map(this::convertInterval).collect(Collectors.toList());
    }

    private String convertInterval(String intervals) {
        String[] array = intervals.split("/");
        if (array.length == 2) {
            return parseTime(array[0]) +
                    "/" +
                    parseTime(array[1]);
        }
        return intervals;
    }

    private String parseTime(String time) {
        String replaced = replaceTime(time);
        Date date = null;
        try {
            date = createInputFormat().parse(replaced);
        } catch (Exception e) {
        }

        if (date == null) {
            try {
                date = createYearFormat().parse(replaced);
            } catch (Exception e) {
            }
        }

        if (date != null) {
            return createOutputFormat().format(date) + TIME_ZONE_OFFSET;
        }
        return time;
    }

    private String replaceTime(String time) {
        int index = time.indexOf('Z');
        if (index > 0) {
            time = time.substring(0, index);
        }
        return time.replace("T", " ");
    }

    private Object convertGranularity(Object granularityValue) {
        Object value = null;
        if (granularityValue instanceof String) {
            String granularity = (String) granularityValue;
            value = GRANULARITY_RESOURCE.get(granularity.toUpperCase());
        } else if (granularityValue instanceof Map) {
            Map<String, Object> map = (Map<String, Object>) granularityValue;
            map.put("timeZone", QUERY_TIME_ZONE);
            return map;
        }
        return value == null ? granularityValue : value;
    }

    private static Map<String, Object> createGranularity(String period) {
        Map<String, Object> map = new HashMap<>();
        map.put("type", "period");
        map.put("timeZone", QUERY_TIME_ZONE);
        map.put("period", period);
        return map;
    }
}
