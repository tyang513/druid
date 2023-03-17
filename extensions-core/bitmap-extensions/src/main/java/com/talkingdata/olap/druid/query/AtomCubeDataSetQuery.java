package com.talkingdata.olap.druid.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.joda.time.Interval;

/**
 * AtomCubeQuery
 *
 * @author minfengxu
 * @version v1
 * @since 2017/4/10
 */
@JsonTypeName("dataset")
public class AtomCubeDataSetQuery extends BaseQuery<AtomCubeRow> {
    public static final String TYPE_NAME = "dataset";
    private static final QuerySegmentSpec NOOP_QUERY_SEGMENT_SPEC = new NoopQuerySegmentSpec();

    private List<String> keys;
    private Operation operation;

    @JsonCreator
    public AtomCubeDataSetQuery(
            @JsonProperty("keys") List<String> keys,
            @JsonProperty("operation") String operation,
            @JsonProperty("context") Map<String, Object> context) {
        super(new TableDataSource(""), NOOP_QUERY_SEGMENT_SPEC, true, context);
        this.keys = keys == null ? new ArrayList<>() : keys;
        this.operation = Operation.valueOf(operation);
    }

    @Override
    public boolean hasFilters() {
        return false;
    }

    @Override
    public DimFilter getFilter() {
        return null;
    }

    @Override
    public String getType() {
        return TYPE_NAME;
    }

    @Override
    public Query<AtomCubeRow> withOverriddenContext(Map<String, Object> contextOverride) {
        return this;
    }

    @Override
    public Query<AtomCubeRow> withQuerySegmentSpec(QuerySegmentSpec spec) {
        return this;
    }

    @JsonProperty("keys")
    public List<String> getKeys() {
        return keys;
    }

    @JsonProperty("operation")
    public String getOperation() {
        return this.operation.name();
    }

    @Override
    public Query<AtomCubeRow> withDataSource(DataSource dataSource) {
        return this;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("AtomCubeDataSetQuery {").append('\n');
        sb.append("\tkeys=");
        sb.append('[');
        for (String key : getKeys()) {
            sb.append(key).append(',');
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(']').append('\n');
        sb.append("\toperation=").append(getOperation()).append('\n');
        sb.append("\tcontext=").append(getContext()).append('\n');
        sb.append('}');
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        AtomCubeDataSetQuery that = (AtomCubeDataSetQuery) o;

        if (keys != null ? !keys.equals(that.keys) : that.keys != null)
            return false;
        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (keys != null ? keys.hashCode() : 0);
        return result;
    }

    enum Operation {
        union,
        intersect,
        not,
        get,
        delete
    }

    private static class NoopQuerySegmentSpec implements QuerySegmentSpec {
        @Override
        public List<Interval> getIntervals() {
            return Collections.singletonList(new Interval(0, 0));
            // return Lists.newArrayList(new Interval(0, 0)); tao.yang
        }

        @Override
        public <T> QueryRunner<T> lookup(Query<T> query, QuerySegmentWalker walker) {
            return walker.getQueryRunnerForSegments(query, null);
        }
    }
}