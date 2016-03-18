package org.apache.cassandraBloomFilters.hints;

import org.apache.cassandraBloomFilters.io.compress.ICompressor;
import org.apache.cassandraBloomFilters.io.util.ChannelProxy;

public class CompressedChecksummedDataInputBuilder extends ChecksummedDataInput.Builder
{
    long position;
    ICompressor compressor;

    public CompressedChecksummedDataInputBuilder(ChannelProxy channel)
    {
        super(channel);
        bufferType = null;
    }

    public ChecksummedDataInput build()
    {
        assert position >= 0;
        assert compressor != null;
        return new CompressedChecksummedDataInput(this);
    }

    public CompressedChecksummedDataInputBuilder withCompressor(ICompressor compressor)
    {
        this.compressor = compressor;
        bufferType = compressor.preferredBufferType();
        return this;
    }

    public CompressedChecksummedDataInputBuilder withPosition(long position)
    {
        this.position = position;
        return this;
    }
}
