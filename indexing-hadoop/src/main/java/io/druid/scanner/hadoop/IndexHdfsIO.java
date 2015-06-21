package io.druid.scanner.hadoop;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.metamx.common.ISE;
import com.metamx.common.io.smoosh.SmooshedHdfsFileMapper;
import com.metamx.emitter.EmittingLogger;
import io.druid.common.utils.SerializerUtils;
import io.druid.guice.ConfigProvider;
import io.druid.guice.GuiceInjectors;
import io.druid.guice.JsonConfigProvider;
import io.druid.query.DruidProcessingConfig;
import io.druid.segment.*;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnConfig;
import io.druid.segment.column.ColumnDescriptor;
import io.druid.segment.data.BitmapSerde;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.Indexed;
import org.apache.commons.compress.archivers.zip.StreamZipFile;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.joda.time.Interval;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Map;

/**
 * Created by babokim on 15. 6. 15..
 */
public class IndexHdfsIO {
  public static final byte V8_VERSION = 0x8;
  public static final byte V9_VERSION = 0x9;
  public static final int CURRENT_VERSION_ID = V9_VERSION;

  public static final ByteOrder BYTE_ORDER = ByteOrder.nativeOrder();

  private static final Map<Integer, IndexLoader> indexLoaders =
      ImmutableMap.<Integer, IndexLoader>builder()
//          .put(0, new LegacyIndexLoader())
//          .put(1, new LegacyIndexLoader())
//          .put(2, new LegacyIndexLoader())
//          .put(3, new LegacyIndexLoader())
//          .put(4, new LegacyIndexLoader())
//          .put(5, new LegacyIndexLoader())
//          .put(6, new LegacyIndexLoader())
//          .put(7, new LegacyIndexLoader())
//          .put(8, new LegacyIndexLoader())
          .put(9, new V9HdfsIndexLoader())
          .build();

  private static final EmittingLogger log = new EmittingLogger(IndexIO.class);
  private static final SerializerUtils serializerUtils = new SerializerUtils();

  private static final ObjectMapper mapper;

  protected static final ColumnConfig columnConfig;

  @Deprecated // specify bitmap type in IndexSpec instead
  protected static final BitmapSerdeFactory CONFIGURED_BITMAP_SERDE_FACTORY;

  static {
    final Injector injector = GuiceInjectors.makeStartupInjectorWithModules(
        ImmutableList.<Module>of(
            new Module() {
              @Override
              public void configure(Binder binder) {
                ConfigProvider.bind(
                    binder,
                    DruidProcessingConfig.class,
                    ImmutableMap.of("base_path", "druid.processing")
                );
                binder.bind(ColumnConfig.class).to(DruidProcessingConfig.class);

                // this property is deprecated, use IndexSpec instead
                JsonConfigProvider.bind(binder, "druid.processing.bitmap", BitmapSerdeFactory.class);
              }
            }
        )
    );
    mapper = injector.getInstance(ObjectMapper.class);
    columnConfig = injector.getInstance(ColumnConfig.class);
    CONFIGURED_BITMAP_SERDE_FACTORY = injector.getInstance(BitmapSerdeFactory.class);
  }

  public static byte[] toByteArray(InputStream in, int length) throws IOException {
    byte[] buf = new byte[length];

    try {
      in.read(buf);
    } finally {
      if (in != null) {
        in.close();
      }
    }

    return buf;
  }

  public static QueryableIndex loadIndex(Configuration conf, Path inDir) throws IOException
  {
    FileSystem fs = inDir.getFileSystem(conf);
    FileStatus fileStatus = fs.getFileStatus(inDir);
    StreamZipFile zipFile = new StreamZipFile(inDir.toString(), fs.open(inDir), fileStatus.getLen());

    final int version = getVersionFromDir(zipFile);

    final IndexLoader loader = indexLoaders.get(version);

    if (loader != null) {
      return loader.load(zipFile, version);
    } else {
      throw new ISE("Unknown index version[%s]", version);
    }
  }

  public static int getVersionFromDir(StreamZipFile zipFile) throws IOException {
    ZipArchiveEntry zipEntry = zipFile.getEntry("version.bin");
    if(zipEntry != null) {
      return Ints.fromByteArray(
          toByteArray(zipFile.getInputStream(zipEntry), (int) zipEntry.getSize()));
    } else {
      throw new IOException("No version file in: " + zipFile.getPath());
    }
  }

  public static void checkFileSize(FileSystem fs, Path indexFile) throws IOException
  {
    FileStatus fileStatus = fs.getFileStatus(indexFile);
    final long fileSize = fileStatus.getLen();
    if (fileSize > Integer.MAX_VALUE) {
      throw new IOException(String.format("File[%s] too large[%s]", indexFile, fileSize));
    }
  }

//  public static boolean convertSegment(File toConvert, File converted, IndexSpec indexSpec) throws IOException
//  {
//    return convertSegment(toConvert, converted, indexSpec, false, true);
//  }
//
//  public static boolean convertSegment(File toConvert, File converted, IndexSpec indexSpec, boolean forceIfCurrent, boolean validate)
//      throws IOException
//  {
//    final int version = SegmentUtils.getVersionFromDir(toConvert);
//    switch (version) {
//      case 1:
//      case 2:
//      case 3:
//        log.makeAlert("Attempt to load segment of version <= 3.")
//            .addData("version", version)
//            .emit();
//        return false;
//      case 4:
//      case 5:
//      case 6:
//      case 7:
//        log.info("Old version, re-persisting.");
//        IndexMerger.append(
//            Arrays.<IndexableAdapter>asList(new QueryableIndexIndexableAdapter(loadIndex(toConvert))),
//            converted,
//            indexSpec
//        );
//        return true;
//      case 8:
//        DefaultIndexIOHandler.convertV8toV9(toConvert, converted, indexSpec);
//        return true;
//      default:
//        if (forceIfCurrent) {
//          IndexMaker.convert(toConvert, converted, indexSpec);
//          if(validate){
//            DefaultIndexIOHandler.validateTwoSegments(toConvert, converted);
//          }
//          return true;
//        } else {
//          log.info("Version[%s], skipping.", version);
//          return false;
//        }
//    }
//  }

  public static interface IndexIOHandler
  {
    public MMappedIndex mapDir(FileSystem fs, Path inDir) throws IOException;
  }

  public static void validateRowValues(
      Rowboat rb1,
      IndexableAdapter adapter1,
      Rowboat rb2,
      IndexableAdapter adapter2
  )
  {
    if(rb1.getTimestamp() != rb2.getTimestamp()){
      throw new SegmentValidationException(
          "Timestamp mismatch. Expected %d found %d",
          rb1.getTimestamp(), rb2.getTimestamp()
      );
    }
    final int[][] dims1 = rb1.getDims();
    final int[][] dims2 = rb2.getDims();
    if (dims1.length != dims2.length) {
      throw new SegmentValidationException(
          "Dim lengths not equal %s vs %s",
          Arrays.deepToString(dims1),
          Arrays.deepToString(dims2)
      );
    }
    final Indexed<String> dim1Names = adapter1.getDimensionNames();
    final Indexed<String> dim2Names = adapter2.getDimensionNames();
    for (int i = 0; i < dims1.length; ++i) {
      final int[] dim1Vals = dims1[i];
      final int[] dim2Vals = dims2[i];
      final String dim1Name = dim1Names.get(i);
      final String dim2Name = dim2Names.get(i);
      final Indexed<String> dim1ValNames = adapter1.getDimValueLookup(dim1Name);
      final Indexed<String> dim2ValNames = adapter2.getDimValueLookup(dim2Name);

      if (dim1Vals == null || dim2Vals == null) {
        if (dim1Vals != dim2Vals) {
          throw new SegmentValidationException(
              "Expected nulls, found %s and %s",
              Arrays.toString(dim1Vals),
              Arrays.toString(dim2Vals)
          );
        } else {
          continue;
        }
      }
      if (dim1Vals.length != dim2Vals.length) {
        // Might be OK if one of them has null. This occurs in IndexMakerTest
        if (dim1Vals.length == 0 && dim2Vals.length == 1) {
          final String dimValName = dim2ValNames.get(dim2Vals[0]);
          if (dimValName == null) {
            continue;
          } else {
            throw new SegmentValidationException(
                "Dim [%s] value [%s] is not null",
                dim2Name,
                dimValName
            );
          }
        } else if (dim2Vals.length == 0 && dim1Vals.length == 1) {
          final String dimValName = dim1ValNames.get(dim1Vals[0]);
          if (dimValName == null) {
            continue;
          } else {
            throw new SegmentValidationException(
                "Dim [%s] value [%s] is not null",
                dim1Name,
                dimValName
            );
          }
        } else {
          throw new SegmentValidationException(
              "Dim [%s] value lengths not equal. Expected %d found %d",
              dim1Name,
              dims1.length,
              dims2.length
          );
        }
      }

      for (int j = 0; j < Math.max(dim1Vals.length, dim2Vals.length); ++j) {
        final int dIdex1 = dim1Vals.length <= j ? -1 : dim1Vals[j];
        final int dIdex2 = dim2Vals.length <= j ? -1 : dim2Vals[j];

        if (dIdex1 == dIdex2) {
          continue;
        }

        final String dim1ValName = dIdex1 < 0 ? null : dim1ValNames.get(dIdex1);
        final String dim2ValName = dIdex2 < 0 ? null : dim2ValNames.get(dIdex2);
        if ((dim1ValName == null) || (dim2ValName == null)) {
          if ((dim1ValName == null) && (dim2ValName == null)) {
            continue;
          } else {
            throw new SegmentValidationException(
                "Dim [%s] value not equal. Expected [%s] found [%s]",
                dim1Name,
                dim1ValName,
                dim2ValName
            );
          }
        }

        if (!dim1ValName.equals(dim2ValName)) {
          throw new SegmentValidationException(
              "Dim [%s] value not equal. Expected [%s] found [%s]",
              dim1Name,
              dim1ValName,
              dim2ValName
          );
        }
      }
    }
  }

//  public static class DefaultIndexIOHandler implements IndexIOHandler
//  {
//    private static final Logger log = new Logger(DefaultIndexIOHandler.class);
//
//    @Override
//    public MMappedIndex mapDir(FileSystem fs, Path inDir) throws IOException
//    {
//      log.debug("Mapping v8 index[%s]", inDir);
//      long startTime = System.currentTimeMillis();
//
//      InputStream indexIn = null;
//      try {
//        indexIn = new FileInputStream(new File(inDir, "index.drd"));
//        byte theVersion = (byte) indexIn.read();
//        if (theVersion != V8_VERSION) {
//          throw new IllegalArgumentException(String.format("Unknown version[%s]", theVersion));
//        }
//      }
//      finally {
//        Closeables.close(indexIn, false);
//      }
//
//      SmooshedFileMapper smooshedFiles = Smoosh.map(inDir);
//      ByteBuffer indexBuffer = smooshedFiles.mapFile("index.drd");
//
//      indexBuffer.get(); // Skip the version byte
//      final GenericIndexed<String> availableDimensions = GenericIndexed.read(
//          indexBuffer, GenericIndexed.STRING_STRATEGY
//      );
//      final GenericIndexed<String> availableMetrics = GenericIndexed.read(
//          indexBuffer, GenericIndexed.STRING_STRATEGY
//      );
//      final Interval dataInterval = new Interval(serializerUtils.readString(indexBuffer));
//      final BitmapSerdeFactory bitmapSerdeFactory = new BitmapSerde.LegacyBitmapSerdeFactory();
//
//      CompressedLongsIndexedSupplier timestamps = CompressedLongsIndexedSupplier.fromByteBuffer(
//          smooshedFiles.mapFile(makeTimeFile(inDir, BYTE_ORDER).getName()), BYTE_ORDER
//      );
//
//      Map<String, MetricHolder> metrics = Maps.newLinkedHashMap();
//      for (String metric : availableMetrics) {
//        final String metricFilename = makeMetricFile(inDir, metric, BYTE_ORDER).getName();
//        final MetricHolder holder = MetricHolder.fromByteBuffer(smooshedFiles.mapFile(metricFilename));
//
//        if (!metric.equals(holder.getName())) {
//          throw new ISE("Metric[%s] loaded up metric[%s] from disk.  File names do matter.", metric, holder.getName());
//        }
//        metrics.put(metric, holder);
//      }
//
//      Map<String, GenericIndexed<String>> dimValueLookups = Maps.newHashMap();
//      Map<String, VSizeIndexed> dimColumns = Maps.newHashMap();
//      Map<String, GenericIndexed<ImmutableBitmap>> bitmaps = Maps.newHashMap();
//
//      for (String dimension : IndexedIterable.create(availableDimensions)) {
//        ByteBuffer dimBuffer = smooshedFiles.mapFile(makeDimFile(inDir, dimension).getName());
//        String fileDimensionName = serializerUtils.readString(dimBuffer);
//        Preconditions.checkState(
//            dimension.equals(fileDimensionName),
//            "Dimension file[%s] has dimension[%s] in it!?",
//            makeDimFile(inDir, dimension),
//            fileDimensionName
//        );
//
//        dimValueLookups.put(dimension, GenericIndexed.read(dimBuffer, GenericIndexed.STRING_STRATEGY));
//        dimColumns.put(dimension, VSizeIndexed.readFromByteBuffer(dimBuffer));
//      }
//
//      ByteBuffer invertedBuffer = smooshedFiles.mapFile("inverted.drd");
//      for (int i = 0; i < availableDimensions.size(); ++i) {
//        bitmaps.put(
//            serializerUtils.readString(invertedBuffer),
//            GenericIndexed.read(invertedBuffer, bitmapSerdeFactory.getObjectStrategy())
//        );
//      }
//
//      Map<String, ImmutableRTree> spatialIndexed = Maps.newHashMap();
//      ByteBuffer spatialBuffer = smooshedFiles.mapFile("spatial.drd");
//      while (spatialBuffer != null && spatialBuffer.hasRemaining()) {
//        spatialIndexed.put(
//            serializerUtils.readString(spatialBuffer),
//            ByteBufferSerializer.read(
//                spatialBuffer,
//                new IndexedRTree.ImmutableRTreeObjectStrategy(bitmapSerdeFactory.getBitmapFactory())
//            )
//        );
//      }
//
//      final MMappedIndex retVal = new MMappedIndex(
//          availableDimensions,
//          availableMetrics,
//          dataInterval,
//          timestamps,
//          metrics,
//          dimValueLookups,
//          dimColumns,
//          bitmaps,
//          spatialIndexed,
//          smooshedFiles
//      );
//
//      log.debug("Mapped v8 index[%s] in %,d millis", inDir, System.currentTimeMillis() - startTime);
//
//      return retVal;
//    }
//
//    public static void validateTwoSegments(File dir1, File dir2) throws IOException
//    {
//      validateTwoSegments(
//          new QueryableIndexIndexableAdapter(loadIndex(dir1)),
//          new QueryableIndexIndexableAdapter(loadIndex(dir2))
//      );
//    }
//
//    public static void validateTwoSegments(final IndexableAdapter adapter1, final IndexableAdapter adapter2)
//    {
//      if (adapter1.getNumRows() != adapter2.getNumRows()) {
//        throw new SegmentValidationException(
//            "Row count mismatch. Expected [%d] found [%d]",
//            adapter1.getNumRows(),
//            adapter2.getNumRows()
//        );
//      }
//      {
//        final Set<String> dimNames1 = Sets.newHashSet(adapter1.getDimensionNames());
//        final Set<String> dimNames2 = Sets.newHashSet(adapter2.getDimensionNames());
//        if (!dimNames1.equals(dimNames2)) {
//          throw new SegmentValidationException(
//              "Dimension names differ. Expected [%s] found [%s]",
//              dimNames1,
//              dimNames2
//          );
//        }
//        final Set<String> metNames1 = Sets.newHashSet(adapter1.getMetricNames());
//        final Set<String> metNames2 = Sets.newHashSet(adapter2.getMetricNames());
//        if (!metNames1.equals(metNames2)) {
//          throw new SegmentValidationException("Metric names differ. Expected [%s] found [%s]", metNames1, metNames2);
//        }
//      }
//      final Iterator<Rowboat> it1 = adapter1.getRows().iterator();
//      final Iterator<Rowboat> it2 = adapter2.getRows().iterator();
//      long row = 0L;
//      while (it1.hasNext()) {
//        if (!it2.hasNext()) {
//          throw new SegmentValidationException("Unexpected end of second adapter");
//        }
//        final Rowboat rb1 = it1.next();
//        final Rowboat rb2 = it2.next();
//        ++row;
//        if (rb1.getRowNum() != rb2.getRowNum()) {
//          throw new SegmentValidationException("Row number mismatch: [%d] vs [%d]", rb1.getRowNum(), rb2.getRowNum());
//        }
//        if (rb1.compareTo(rb2) != 0) {
//          try {
//            validateRowValues(rb1, adapter1, rb2, adapter2);
//          }
//          catch (SegmentValidationException ex) {
//            throw new SegmentValidationException(ex, "Validation failure on row %d: [%s] vs [%s]", row, rb1, rb2);
//          }
//        }
//      }
//      if (it2.hasNext()) {
//        throw new SegmentValidationException("Unexpected end of first adapter");
//      }
//      if (row != adapter1.getNumRows()) {
//        throw new SegmentValidationException(
//            "Actual Row count mismatch. Expected [%d] found [%d]",
//            row,
//            adapter1.getNumRows()
//        );
//      }
//    }
//
//    public static void convertV8toV9(File v8Dir, File v9Dir, IndexSpec indexSpec) throws IOException
//    {
//      log.info("Converting v8[%s] to v9[%s]", v8Dir, v9Dir);
//
//      InputStream indexIn = null;
//      try {
//        indexIn = new FileInputStream(new File(v8Dir, "index.drd"));
//        byte theVersion = (byte) indexIn.read();
//        if (theVersion != V8_VERSION) {
//          throw new IAE("Unknown version[%s]", theVersion);
//        }
//      }
//      finally {
//        Closeables.close(indexIn, false);
//      }
//
//      SmooshedFileMapper v8SmooshedFiles = Smoosh.map(v8Dir);
//
//      v9Dir.mkdirs();
//      final FileSmoosher v9Smoosher = new FileSmoosher(v9Dir);
//
//      ByteStreams.write(Ints.toByteArray(9), Files.newOutputStreamSupplier(new File(v9Dir, "version.bin")));
//
//      Map<String, GenericIndexed<ImmutableBitmap>> bitmapIndexes = Maps.newHashMap();
//      final ByteBuffer invertedBuffer = v8SmooshedFiles.mapFile("inverted.drd");
//      BitmapSerdeFactory bitmapSerdeFactory = indexSpec.getBitmapSerdeFactory();
//
//      while (invertedBuffer.hasRemaining()) {
//        final String dimName = serializerUtils.readString(invertedBuffer);
//        bitmapIndexes.put(
//            dimName,
//            GenericIndexed.read(invertedBuffer, bitmapSerdeFactory.getObjectStrategy())
//        );
//      }
//
//      Map<String, ImmutableRTree> spatialIndexes = Maps.newHashMap();
//      final ByteBuffer spatialBuffer = v8SmooshedFiles.mapFile("spatial.drd");
//      while (spatialBuffer != null && spatialBuffer.hasRemaining()) {
//        spatialIndexes.put(
//            serializerUtils.readString(spatialBuffer),
//            ByteBufferSerializer.read(
//                spatialBuffer, new IndexedRTree.ImmutableRTreeObjectStrategy(
//                    bitmapSerdeFactory.getBitmapFactory()
//                )
//            )
//        );
//      }
//
//      final LinkedHashSet<String> skippedFiles = Sets.newLinkedHashSet();
//      final Set<String> skippedDimensions = Sets.newLinkedHashSet();
//      for (String filename : v8SmooshedFiles.getInternalFilenames()) {
//        log.info("Processing file[%s]", filename);
//        if (filename.startsWith("dim_")) {
//          final ColumnDescriptor.Builder builder = ColumnDescriptor.builder();
//          builder.setValueType(ValueType.STRING);
//
//          final List<ByteBuffer> outParts = Lists.newArrayList();
//
//          ByteBuffer dimBuffer = v8SmooshedFiles.mapFile(filename);
//          String dimension = serializerUtils.readString(dimBuffer);
//          if (!filename.equals(String.format("dim_%s.drd", dimension))) {
//            throw new ISE("loaded dimension[%s] from file[%s]", dimension, filename);
//          }
//
//          ByteArrayOutputStream nameBAOS = new ByteArrayOutputStream();
//          serializerUtils.writeString(nameBAOS, dimension);
//          outParts.add(ByteBuffer.wrap(nameBAOS.toByteArray()));
//
//          GenericIndexed<String> dictionary = GenericIndexed.read(
//              dimBuffer, GenericIndexed.STRING_STRATEGY
//          );
//
//          if (dictionary.size() == 0) {
//            log.info("Dimension[%s] had cardinality 0, equivalent to no column, so skipping.", dimension);
//            skippedDimensions.add(dimension);
//            continue;
//          }
//
//          List<Integer> singleValCol = null;
//          VSizeIndexed multiValCol = VSizeIndexed.readFromByteBuffer(dimBuffer.asReadOnlyBuffer());
//          GenericIndexed<ImmutableBitmap> bitmaps = bitmapIndexes.get(dimension);
//          ImmutableRTree spatialIndex = spatialIndexes.get(dimension);
//
//          final BitmapFactory bitmapFactory = bitmapSerdeFactory.getBitmapFactory();
//          boolean onlyOneValue = true;
//          MutableBitmap nullsSet = null;
//          for (int i = 0; i < multiValCol.size(); ++i) {
//            VSizeIndexedInts rowValue = multiValCol.get(i);
//            if (!onlyOneValue) {
//              break;
//            }
//            if (rowValue.size() > 1) {
//              onlyOneValue = false;
//            }
//            if (rowValue.size() == 0) {
//              if (nullsSet == null) {
//                nullsSet = bitmapFactory.makeEmptyMutableBitmap();
//              }
//              nullsSet.add(i);
//            }
//          }
//
//          if (onlyOneValue) {
//            log.info("Dimension[%s] is single value, converting...", dimension);
//            final boolean bumpedDictionary;
//            if (nullsSet != null) {
//              log.info("Dimension[%s] has null rows.", dimension);
//              final ImmutableBitmap theNullSet = bitmapFactory.makeImmutableBitmap(nullsSet);
//
//              if (dictionary.get(0) != null) {
//                log.info("Dimension[%s] has no null value in the dictionary, expanding...", dimension);
//                bumpedDictionary = true;
//                final List<String> nullList = Lists.newArrayList();
//                nullList.add(null);
//
//                dictionary = GenericIndexed.fromIterable(
//                    Iterables.concat(nullList, dictionary),
//                    GenericIndexed.STRING_STRATEGY
//                );
//
//                bitmaps = GenericIndexed.fromIterable(
//                    Iterables.concat(Arrays.asList(theNullSet), bitmaps),
//                    bitmapSerdeFactory.getObjectStrategy()
//                );
//              } else {
//                bumpedDictionary = false;
//                bitmaps = GenericIndexed.fromIterable(
//                    Iterables.concat(
//                        Arrays.asList(
//                            bitmapFactory
//                                .union(Arrays.asList(theNullSet, bitmaps.get(0)))
//                        ),
//                        Iterables.skip(bitmaps, 1)
//                    ),
//                    bitmapSerdeFactory.getObjectStrategy()
//                );
//              }
//            } else {
//              bumpedDictionary = false;
//            }
//
//            final VSizeIndexed finalMultiValCol = multiValCol;
//            singleValCol = new AbstractList<Integer>()
//            {
//              @Override
//              public Integer get(int index)
//              {
//                final VSizeIndexedInts ints = finalMultiValCol.get(index);
//                return ints.size() == 0 ? 0 : ints.get(0) + (bumpedDictionary ? 1 : 0);
//              }
//
//              @Override
//              public int size()
//              {
//                return finalMultiValCol.size();
//              }
//            };
//
//            multiValCol = null;
//          } else {
//            builder.setHasMultipleValues(true);
//          }
//
//          final CompressedObjectStrategy.CompressionStrategy compressionStrategy = indexSpec.getDimensionCompressionStrategy();
//
//          final DictionaryEncodedColumnPartSerde.Builder columnPartBuilder = DictionaryEncodedColumnPartSerde
//              .builder()
//              .withDictionary(dictionary)
//              .withBitmapSerdeFactory(bitmapSerdeFactory)
//              .withBitmaps(bitmaps)
//              .withSpatialIndex(spatialIndex)
//              .withByteOrder(BYTE_ORDER);
//
//          if (singleValCol != null) {
//            if (compressionStrategy != null) {
//              columnPartBuilder.withSingleValuedColumn(
//                  CompressedVSizeIntsIndexedSupplier.fromList(
//                      singleValCol,
//                      dictionary.size(),
//                      CompressedVSizeIntsIndexedSupplier.maxIntsInBufferForValue(dictionary.size()),
//                      BYTE_ORDER,
//                      compressionStrategy
//                  )
//              );
//            } else {
//              columnPartBuilder.withSingleValuedColumn(VSizeIndexedInts.fromList(singleValCol, dictionary.size()));
//            }
//          } else {
//            if (compressionStrategy != null) {
//              log.info(
//                  "Compression not supported for multi-value dimensions, defaulting to `uncompressed` for dimension[%s]",
//                  dimension
//              );
//            }
//            columnPartBuilder.withMultiValuedColumn(multiValCol);
//          }
//
//          final ColumnDescriptor serdeficator = builder
//              .addSerde(columnPartBuilder.build())
//              .build();
//
//          ByteArrayOutputStream baos = new ByteArrayOutputStream();
//          serializerUtils.writeString(baos, mapper.writeValueAsString(serdeficator));
//          byte[] specBytes = baos.toByteArray();
//
//          final SmooshedWriter channel = v9Smoosher.addWithSmooshedWriter(
//              dimension, serdeficator.numBytes() + specBytes.length
//          );
//          channel.write(ByteBuffer.wrap(specBytes));
//          serdeficator.write(channel);
//          channel.close();
//        } else if (filename.startsWith("met_")) {
//          if (!filename.endsWith(String.format("%s.drd", BYTE_ORDER))) {
//            skippedFiles.add(filename);
//            continue;
//          }
//
//          MetricHolder holder = MetricHolder.fromByteBuffer(v8SmooshedFiles.mapFile(filename));
//          final String metric = holder.getName();
//
//          final ColumnDescriptor.Builder builder = ColumnDescriptor.builder();
//
//          switch (holder.getType()) {
//            case LONG:
//              builder.setValueType(ValueType.LONG);
//              builder.addSerde(new LongGenericColumnPartSerde(holder.longType, BYTE_ORDER));
//              break;
//            case FLOAT:
//              builder.setValueType(ValueType.FLOAT);
//              builder.addSerde(new FloatGenericColumnPartSerde(holder.floatType, BYTE_ORDER));
//              break;
//            case COMPLEX:
//              if (!(holder.complexType instanceof GenericIndexed)) {
//                throw new ISE("Serialized complex types must be GenericIndexed objects.");
//              }
//              final GenericIndexed column = (GenericIndexed) holder.complexType;
//              final String complexType = holder.getTypeName();
//
//              builder.setValueType(ValueType.COMPLEX);
//              builder.addSerde(new ComplexColumnPartSerde(column, complexType));
//              break;
//            default:
//              throw new ISE("Unknown type[%s]", holder.getType());
//          }
//
//          final ColumnDescriptor serdeficator = builder.build();
//
//          ByteArrayOutputStream baos = new ByteArrayOutputStream();
//          serializerUtils.writeString(baos, mapper.writeValueAsString(serdeficator));
//          byte[] specBytes = baos.toByteArray();
//
//          final SmooshedWriter channel = v9Smoosher.addWithSmooshedWriter(
//              metric, serdeficator.numBytes() + specBytes.length
//          );
//          channel.write(ByteBuffer.wrap(specBytes));
//          serdeficator.write(channel);
//          channel.close();
//        } else if (String.format("time_%s.drd", BYTE_ORDER).equals(filename)) {
//          CompressedLongsIndexedSupplier timestamps = CompressedLongsIndexedSupplier.fromByteBuffer(
//              v8SmooshedFiles.mapFile(filename), BYTE_ORDER
//          );
//
//          final ColumnDescriptor.Builder builder = ColumnDescriptor.builder();
//          builder.setValueType(ValueType.LONG);
//          builder.addSerde(new LongGenericColumnPartSerde(timestamps, BYTE_ORDER));
//
//          final ColumnDescriptor serdeficator = builder.build();
//
//          ByteArrayOutputStream baos = new ByteArrayOutputStream();
//          serializerUtils.writeString(baos, mapper.writeValueAsString(serdeficator));
//          byte[] specBytes = baos.toByteArray();
//
//          final SmooshedWriter channel = v9Smoosher.addWithSmooshedWriter(
//              "__time", serdeficator.numBytes() + specBytes.length
//          );
//          channel.write(ByteBuffer.wrap(specBytes));
//          serdeficator.write(channel);
//          channel.close();
//        } else {
//          skippedFiles.add(filename);
//        }
//      }
//
//      final ByteBuffer indexBuffer = v8SmooshedFiles.mapFile("index.drd");
//
//      indexBuffer.get(); // Skip the version byte
//      final GenericIndexed<String> dims8 = GenericIndexed.read(
//          indexBuffer, GenericIndexed.STRING_STRATEGY
//      );
//      final GenericIndexed<String> dims9 = GenericIndexed.fromIterable(
//          Iterables.filter(
//              dims8, new Predicate<String>()
//              {
//                @Override
//                public boolean apply(String s)
//                {
//                  return !skippedDimensions.contains(s);
//                }
//              }
//          ),
//          GenericIndexed.STRING_STRATEGY
//      );
//      final GenericIndexed<String> availableMetrics = GenericIndexed.read(
//          indexBuffer, GenericIndexed.STRING_STRATEGY
//      );
//      final Interval dataInterval = new Interval(serializerUtils.readString(indexBuffer));
//      final BitmapSerdeFactory segmentBitmapSerdeFactory = mapper.readValue(
//          serializerUtils.readString(indexBuffer),
//          BitmapSerdeFactory.class
//      );
//
//      Set<String> columns = Sets.newTreeSet();
//      columns.addAll(Lists.newArrayList(dims9));
//      columns.addAll(Lists.newArrayList(availableMetrics));
//
//      GenericIndexed<String> cols = GenericIndexed.fromIterable(columns, GenericIndexed.STRING_STRATEGY);
//
//      final String segmentBitmapSerdeFactoryString = mapper.writeValueAsString(segmentBitmapSerdeFactory);
//
//      final long numBytes = cols.getSerializedSize() + dims9.getSerializedSize() + 16
//          + serializerUtils.getSerializedStringByteSize(segmentBitmapSerdeFactoryString);
//
//      final SmooshedWriter writer = v9Smoosher.addWithSmooshedWriter("index.drd", numBytes);
//      cols.writeToChannel(writer);
//      dims9.writeToChannel(writer);
//      serializerUtils.writeLong(writer, dataInterval.getStartMillis());
//      serializerUtils.writeLong(writer, dataInterval.getEndMillis());
//      serializerUtils.writeString(writer, segmentBitmapSerdeFactoryString);
//      writer.close();
//
//      log.info("Skipped files[%s]", skippedFiles);
//
//      v9Smoosher.close();
//    }
//  }
//
  static interface IndexLoader
  {
    public QueryableIndex load(StreamZipFile zipFile, int version) throws IOException;
  }

//  static class LegacyIndexLoader implements IndexLoader
//  {
//    private static final IndexIOHandler legacyHandler = new DefaultIndexIOHandler();
//
//    @Override
//    public QueryableIndex load(FileSystem fs, Path inDir) throws IOException
//    {
//      MMappedIndex index = legacyHandler.mapDir(inDir);
//
//      Map<String, Column> columns = Maps.newHashMap();
//
//      for (String dimension : index.getAvailableDimensions()) {
//        ColumnBuilder builder = new ColumnBuilder()
//            .setType(ValueType.STRING)
//            .setHasMultipleValues(true)
//            .setDictionaryEncodedColumn(
//                new DictionaryEncodedColumnSupplier(
//                    index.getDimValueLookup(dimension),
//                    null,
//                    Suppliers.<IndexedMultivalue<IndexedInts>>ofInstance(
//                        index.getDimColumn(dimension)
//                    ),
//                    columnConfig.columnCacheSizeBytes()
//                )
//            )
//            .setBitmapIndex(
//                new BitmapIndexColumnPartSupplier(
//                    new ConciseBitmapFactory(),
//                    index.getBitmapIndexes().get(dimension),
//                    index.getDimValueLookup(dimension)
//                )
//            );
//        if (index.getSpatialIndexes().get(dimension) != null) {
//          builder.setSpatialIndex(
//              new SpatialIndexColumnPartSupplier(
//                  index.getSpatialIndexes().get(dimension)
//              )
//          );
//        }
//        columns.put(
//            dimension,
//            builder.build()
//        );
//      }
//
//      for (String metric : index.getAvailableMetrics()) {
//        final MetricHolder metricHolder = index.getMetricHolder(metric);
//        if (metricHolder.getType() == MetricHolder.MetricType.FLOAT) {
//          columns.put(
//              metric,
//              new ColumnBuilder()
//                  .setType(ValueType.FLOAT)
//                  .setGenericColumn(new FloatGenericColumnSupplier(metricHolder.floatType, BYTE_ORDER))
//                  .build()
//          );
//        } else if (metricHolder.getType() == MetricHolder.MetricType.COMPLEX) {
//          columns.put(
//              metric,
//              new ColumnBuilder()
//                  .setType(ValueType.COMPLEX)
//                  .setComplexColumn(
//                      new ComplexColumnPartSupplier(
//                          metricHolder.getTypeName(), (GenericIndexed) metricHolder.complexType
//                      )
//                  )
//                  .build()
//          );
//        }
//      }
//
//      Set<String> colSet = Sets.newTreeSet();
//      for (String dimension : index.getAvailableDimensions()) {
//        colSet.add(dimension);
//      }
//      for (String metric : index.getAvailableMetrics()) {
//        colSet.add(metric);
//      }
//
//      String[] cols = colSet.toArray(new String[colSet.size()]);
//      columns.put(
//          Column.TIME_COLUMN_NAME, new ColumnBuilder()
//              .setType(ValueType.LONG)
//              .setGenericColumn(new LongGenericColumnSupplier(index.timestamps))
//              .build()
//      );
//      return new SimpleQueryableIndex(
//          index.getDataInterval(),
//          new ArrayIndexed<>(cols, String.class),
//          index.getAvailableDimensions(),
//          new ConciseBitmapFactory(),
//          columns,
//          index.getFileMapper()
//      );
//    }
//  }
//
  static class V9HdfsIndexLoader implements IndexLoader
  {
    @Override
    public QueryableIndex load(StreamZipFile zipFile, final int theVersion) throws IOException
    {
      log.debug("Mapping v9 index[%s]", zipFile.getPath());
      long startTime = System.currentTimeMillis();

      if (theVersion != V9_VERSION) {
        throw new IllegalArgumentException(String.format("Expected version[9], got[%s]", theVersion));
      }

      SmooshedHdfsFileMapper smooshedFiles = SmooshedHdfsFileMapper.load(zipFile);

      ByteBuffer indexBuffer = smooshedFiles.mapFile("index.drd");
      /**
       * Index.drd should consist of the segment version, the columns and dimensions of the segment as generic
       * indexes, the interval start and end millis as longs (in 16 bytes), and a bitmap index type.
       */
      final GenericIndexed<String> cols = GenericIndexed.read(indexBuffer, GenericIndexed.STRING_STRATEGY);
      final GenericIndexed<String> dims = GenericIndexed.read(indexBuffer, GenericIndexed.STRING_STRATEGY);
      final Interval dataInterval = new Interval(indexBuffer.getLong(), indexBuffer.getLong());
      final BitmapSerdeFactory segmentBitmapSerdeFactory;
      /**
       * This is a workaround for the fact that in v8 segments, we have no information about the type of bitmap
       * index to use. Since we cannot very cleanly build v9 segments directly, we are using a workaround where
       * this information is appended to the end of index.drd.
       */
      if (indexBuffer.hasRemaining()) {
        segmentBitmapSerdeFactory = mapper.readValue(serializerUtils.readString(indexBuffer), BitmapSerdeFactory.class);
      } else {
        segmentBitmapSerdeFactory = new BitmapSerde.LegacyBitmapSerdeFactory();
      }

      Map<String, Column> columns = Maps.newHashMap();

      for (String columnName : cols) {
        columns.put(columnName, deserializeColumn(mapper, smooshedFiles.mapFile(columnName)));
      }

      columns.put(Column.TIME_COLUMN_NAME, deserializeColumn(mapper, smooshedFiles.mapFile("__time")));

      final QueryableIndex index = new SimpleQueryableHdfsIndex(
          dataInterval, cols, dims, segmentBitmapSerdeFactory.getBitmapFactory(), columns, smooshedFiles
      );

      log.debug("Mapped v9 index[%s] in %,d millis", zipFile.getPath(), System.currentTimeMillis() - startTime);

      return index;
    }

    private Column deserializeColumn(ObjectMapper mapper, ByteBuffer byteBuffer) throws IOException
    {
      ColumnDescriptor serde = mapper.readValue(
          serializerUtils.readString(byteBuffer), ColumnDescriptor.class
      );
      return serde.read(byteBuffer, columnConfig);
    }
  }

//  public static File makeDimFile(File dir, String dimension)
//  {
//    return new File(dir, String.format("dim_%s.drd", dimension));
//  }
//
//  public static File makeTimeFile(File dir, ByteOrder order)
//  {
//    return new File(dir, String.format("time_%s.drd", order));
//  }
//
//  public static File makeMetricFile(File dir, String metricName, ByteOrder order)
//  {
//    return new File(dir, String.format("met_%s_%s.drd", metricName, order));
//  }
}