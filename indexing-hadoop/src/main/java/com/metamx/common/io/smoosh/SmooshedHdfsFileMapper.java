package com.metamx.common.io.smoosh;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import com.metamx.common.ByteBufferUtils;
import com.metamx.common.ISE;
import org.apache.commons.compress.archivers.zip.StreamZipFile;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class SmooshedHdfsFileMapper implements Closeable {
  private final List<String> outFiles;
  private final Map<String, Metadata> internalFiles;
  private final List<ByteBuffer> buffersList = Lists.newArrayList();
  private final StreamZipFile zipFile;

  public static SmooshedHdfsFileMapper load(StreamZipFile zipFile) throws IOException {
    String metaFile = "meta.smoosh";
    ZipArchiveEntry entry = zipFile.getEntry(metaFile);
    if (entry == null) {
      throw new IOException("No meta file in " + zipFile.getPath());
    }
    BufferedReader in = null;

    SmooshedHdfsFileMapper var8;
    try {
      in = new BufferedReader(new InputStreamReader(zipFile.getInputStream(entry), Charsets.UTF_8));
      String line = in.readLine();
      if(line == null) {
        throw new ISE("First line should be version,maxChunkSize,numChunks, got null.", new Object[0]);
      }

      String[] splits = line.split(",");
      if(!"v1".equals(splits[0])) {
        throw new ISE("Unknown version[%s], v1 is all I know.", new Object[]{splits[0]});
      }

      if(splits.length != 3) {
        throw new ISE("Wrong number of splits[%d] in line[%s]", new Object[]{Integer.valueOf(splits.length), line});
      }

      Integer numFiles = Integer.valueOf(splits[2]);
      List<String> outFiles = Lists.newArrayListWithExpectedSize(numFiles.intValue());

      for(int internalFiles = 0; internalFiles < numFiles.intValue(); ++internalFiles) {
        outFiles.add(String.format("%05d.%s", new Object[]{Integer.valueOf(internalFiles), "smoosh"}));
      }

      TreeMap var12 = Maps.newTreeMap();

      while((line = in.readLine()) != null) {
        splits = line.split(",");
        if(splits.length != 4) {
          throw new ISE("Wrong number of splits[%d] in line[%s]", new Object[]{Integer.valueOf(splits.length), line});
        }

        var12.put(splits[0], new Metadata(Integer.parseInt(splits[1]), Integer.parseInt(splits[2]), Integer.parseInt(splits[3])));
      }

      var8 = new SmooshedHdfsFileMapper(zipFile, outFiles, var12);
    } finally {
      Closeables.close(in, false);
    }

    return var8;
  }

  private SmooshedHdfsFileMapper(StreamZipFile zipFile, List<String> outFiles, Map<String, Metadata> internalFiles) {
    this.outFiles = outFiles;
    this.internalFiles = internalFiles;
    this.zipFile = zipFile;
  }

  public ByteBuffer mapFile(String name) throws IOException {
    Metadata metadata = this.internalFiles.get(name);
    if(metadata == null) {
      return null;
    } else {
      int fileNum = metadata.getFileNum();

      while(this.buffersList.size() <= fileNum) {
        this.buffersList.add(null);
      }

      ByteBuffer buffer = this.buffersList.get(fileNum);
      if(buffer == null) {
        ZipArchiveEntry entry = zipFile.getEntry(this.outFiles.get(fileNum));
        if (entry == null) {
          throw new IOException("No data file[ " + this.outFiles.get(fileNum) + "] in " + zipFile.getPath());
        }
        ReadableByteChannel channel = null;
        try {
          channel = Channels.newChannel(zipFile.getInputStream(entry));
          buffer = ByteBuffer.allocateDirect(metadata.getEndOffset());
          channel.read(buffer);
        } finally {
          if (channel != null) {
            channel.close();
          }
        }
        this.buffersList.set(fileNum, buffer);
      }

      buffer.position(metadata.getStartOffset()).limit(metadata.getEndOffset());
      return buffer.slice();
    }
  }

  public void close() throws IOException {
    Iterator i$ = this.buffersList.iterator();

    while(i$.hasNext()) {
      ByteBuffer byteBuffer = (ByteBuffer)i$.next();
      ByteBufferUtils.free(byteBuffer);
    }
    buffersList.clear();
    zipFile.close();
  }
}
