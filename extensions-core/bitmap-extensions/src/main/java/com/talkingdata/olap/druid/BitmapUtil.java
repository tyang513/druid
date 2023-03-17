package com.talkingdata.olap.druid;

import com.google.common.io.BaseEncoding;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.BufferFastAggregation;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

/**
 * BitmapUtil
 *
 * @author bingxin.li
 * @version v1
 * @since 2022-07-12
 */
public class BitmapUtil {
    public static final Comparator<RoaringBitmap> COMPARATOR =
            Comparator.nullsFirst(Comparator.comparing(RoaringBitmap::getLongSizeInBytes));

    public static final Comparator<MutableRoaringBitmap> MRB_COMPARATOR =
            Comparator.nullsFirst(Comparator.comparing(MutableRoaringBitmap::getLongSizeInBytes));

    public static final MutableRoaringBitmap EMPTY_BITMAP = new MutableRoaringBitmap();

    public static RoaringBitmap fromList(List<?> list) {
        RoaringBitmap bitmap = new RoaringBitmap();
        if (list != null && list.size() > 0) {
            for (Object item : list) {
                try {
                    int value = Integer.parseInt(String.valueOf(item));
                    bitmap.add(value);
                } catch (Exception e) {
                }
            }
        }
        return bitmap;
    }

    public static byte[] toBytes(RoaringBitmap bitmap) {
        int size;
        if (bitmap == null || (size = bitmap.serializedSizeInBytes()) == 0) {
            return new byte[0];
        }

        ByteBuffer byteBuffer = ByteBuffer.allocate(size);
        bitmap.serialize(byteBuffer);
        byteBuffer.flip();
        byte[] array = new byte[size];
        byteBuffer.get(array);
        return array;
    }

    public static String toBase64(RoaringBitmap bitmap) {
        return BaseEncoding.base64().encode(toBytes(bitmap));
    }

    public static RoaringBitmap fromBytes(byte[] bytes) {
        return fromBytes(ByteBuffer.wrap(bytes));

    }

    public static RoaringBitmap fromBytes(ByteBuffer byteBuffer) {
        try {
            RoaringBitmap bitmap = new RoaringBitmap();
            bitmap.deserialize(byteBuffer);
            return bitmap;
        } catch (IOException e) {
            throw new ParseException(e, "Deserialized bytes to bitmap error");
        }
    }

    public static RoaringBitmap union(RoaringBitmap... bitmaps) {
        return RoaringBitmap.or(bitmaps);
    }

    public static MutableRoaringBitmap union(MutableRoaringBitmap... bitmaps) {
        return BufferFastAggregation.or(Arrays.asList(bitmaps).iterator());
    }

    public static MutableRoaringBitmap union(List<MutableRoaringBitmap> bitmaps) {
        return BufferFastAggregation.or(bitmaps.iterator());
    }

    public static MutableRoaringBitmap and(MutableRoaringBitmap... bitmaps) {
        return BufferFastAggregation.and(Arrays.asList(bitmaps).iterator());
    }

    public static MutableRoaringBitmap and(List<MutableRoaringBitmap> bitmaps) {
        return BufferFastAggregation.and(bitmaps.iterator());
    }

    public static MutableRoaringBitmap andNot(MutableRoaringBitmap... bitmaps) {
        return andNot(Arrays.asList(bitmaps));
    }

    public static MutableRoaringBitmap andNot(List<MutableRoaringBitmap> bitmaps) {
        if (bitmaps == null || bitmaps.isEmpty()) {
            return new MutableRoaringBitmap();
        }

        int size = bitmaps.size();
        if (size == 1) {
            return bitmaps.get(0);
        }

        List<MutableRoaringBitmap> otherBitmaps = bitmaps.subList(1, size);
        MutableRoaringBitmap otherBitmap = BufferFastAggregation.or(otherBitmaps.iterator());
        return MutableRoaringBitmap.andNot(bitmaps.get(0), otherBitmap);
    }
}
