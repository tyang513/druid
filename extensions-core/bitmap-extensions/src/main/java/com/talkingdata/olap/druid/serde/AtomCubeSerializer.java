package com.talkingdata.olap.druid.serde;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.talkingdata.olap.druid.BitmapUtil;
import java.io.IOException;
import org.roaringbitmap.RoaringBitmap;

/**
 * AtomCubeSerializer
 *
 * @author bingxin.li
 * @version v1
 * @since 2022-07-08
 */
public class AtomCubeSerializer<T extends RoaringBitmap> extends JsonSerializer<T> {
    @Override
    public void serialize(RoaringBitmap value, JsonGenerator jsonGenerator,
                          SerializerProvider provider) throws IOException {
        jsonGenerator.writeBinary(BitmapUtil.toBytes(value));
    }
}
