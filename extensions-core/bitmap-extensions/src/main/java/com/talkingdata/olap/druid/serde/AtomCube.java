package com.talkingdata.olap.druid.serde;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.talkingdata.olap.druid.BitmapUtil;
import java.nio.ByteBuffer;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.segment.GenericColumnSerializer;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.ObjectStrategy;
import org.apache.druid.segment.serde.ComplexColumnPartSupplier;
import org.apache.druid.segment.serde.ComplexMetricExtractor;
import org.apache.druid.segment.serde.ComplexMetricSerde;
import org.apache.druid.segment.serde.LargeColumnSupportedComplexColumnSerializer;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;
import org.roaringbitmap.RoaringBitmap;

/**
 * AtomCube
 *
 * @author bingxin.li
 * @version v1
 * @since 2022-07-08
 */
@JsonTypeName("atomCube")
public class AtomCube extends ComplexMetricSerde {
    public static final String TYPE_NAME = "atomCube";

    @Override
    public String getTypeName() {
        return TYPE_NAME;
    }

    @Override
    public ComplexMetricExtractor<RoaringBitmap> getExtractor() {
        return new BitmapExtractor();
    }

    @Override
    public ObjectStrategy getObjectStrategy() {
        return new BitmapStrategy();
    }

    @Override
    public void deserializeColumn(ByteBuffer byteBuffer, ColumnBuilder columnBuilder) {
        GenericIndexed column = GenericIndexed.read(byteBuffer, new BitmapStrategy(), columnBuilder.getFileMapper());
        columnBuilder.setComplexColumnSupplier(new ComplexColumnPartSupplier(getTypeName(), column));
    }

    @Override
    public GenericColumnSerializer getSerializer(SegmentWriteOutMedium segmentWriteOutMedium, String column) {
        return LargeColumnSupportedComplexColumnSerializer.create(segmentWriteOutMedium, column, this.getObjectStrategy());
    }

    private static class BitmapExtractor implements ComplexMetricExtractor<RoaringBitmap> {
        @Override
        public Class<RoaringBitmap> extractedClass() {
            return RoaringBitmap.class;
        }

        @Override
        public RoaringBitmap extractValue(InputRow inputRow, String metricName) {
            Object value = inputRow.getRaw(metricName);
            return value instanceof RoaringBitmap ? (RoaringBitmap) value : BitmapUtil.fromList(inputRow.getDimension(metricName));
        }
    }

    private static class BitmapStrategy implements ObjectStrategy<RoaringBitmap> {
        @Override
        public Class<RoaringBitmap> getClazz() {
            return RoaringBitmap.class;
        }

        @Override
        public RoaringBitmap fromByteBuffer(ByteBuffer buffer, int numBytes) {

            final ByteBuffer readOnlyBuffer = buffer.asReadOnlyBuffer();
            readOnlyBuffer.limit(readOnlyBuffer.position() + numBytes);
            return BitmapUtil.fromBytes(readOnlyBuffer);
        }

        @Override
        public byte[] toBytes(RoaringBitmap collector) {
            if (collector == null) {
                return new byte[]{};
            }

            return BitmapUtil.toBytes(collector);
        }

        @Override
        public int compare(RoaringBitmap o1, RoaringBitmap o2) {
            return BitmapUtil.COMPARATOR.compare(o1, o2);
        }
    }
}
