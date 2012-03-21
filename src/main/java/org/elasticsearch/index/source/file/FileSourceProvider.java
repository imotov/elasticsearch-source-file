package org.elasticsearch.index.source.file;

import org.elasticsearch.common.BytesHolder;
import org.elasticsearch.common.base.Objects;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.index.mapper.MergeContext;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.source.SourceProvider;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class FileSourceProvider implements SourceProvider {

    protected final ESLogger logger = Loggers.getLogger(getClass());

    public static class Defaults {
        public static final String NAME = "file";
        public static final String ROOT_PATH = "/";
        public static final String PATH_FIELD = "path";
    }

    private String rootPath;

    private final String pathField;

    protected FileSourceProvider(String rootPath, String pathField) {
        this.rootPath = rootPath;
        this.pathField = pathField;
    }

    @Override
    public String name() {
        return Defaults.NAME;
    }

    @Override
    public BytesHolder dehydrateSource(ParseContext context) throws IOException {
        Tuple<XContentType, Map<String, Object>> mapTuple = XContentHelper.convertToMap(context.source(),
                context.sourceOffset(), context.sourceLength(), true);
        Map<String, Object> sourceMap = mapTuple.v2();
        Object pathFieldObject = sourceMap.get(pathField);
        if (pathFieldObject != null && pathFieldObject instanceof String) {
            // Replace path with just the portion that is needed to restore source in the future
            Map<String, Object> dehydratedMap = Maps.newHashMap();
            dehydratedMap.put(pathField, pathFieldObject);
            XContentBuilder builder = XContentFactory.contentBuilder(mapTuple.v1()).map(dehydratedMap);
            return new BytesHolder(builder.copiedBytes());
        } else {
            // Path field is not found - don't store source at all
            return null;
        }
    }

    @Override
    public BytesHolder rehydrateSource(String type, String id, byte[] source, int sourceOffset, int sourceLength) {
        if (source != null) {
            // Extract file path from source
            Tuple<XContentType, Map<String, Object>> mapTuple = XContentHelper.convertToMap(source, sourceOffset, sourceLength, true);
            Map<String, Object> sourceMap = mapTuple.v2();
            Object pathFieldObject = sourceMap.get(pathField);
            if (pathFieldObject != null && pathFieldObject instanceof String) {
                // Load source from the path
                return loadFile((String) pathFieldObject);
            } else {
                // Path field is not found - don't load source
                return null;
            }
        }
        return null;
    }

    @Override
    public void merge(SourceProvider mergeWith, MergeContext mergeContext) {
        if (mergeWith instanceof FileSourceProvider) {
            FileSourceProvider sourceProviderMergeWith = (FileSourceProvider) mergeWith;
            // Allow changing rootPath
            if (sourceProviderMergeWith.rootPath != null) {
                this.rootPath = sourceProviderMergeWith.rootPath;
            }
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        if (!Objects.equal(rootPath, Defaults.ROOT_PATH)) {
            builder.field("root_path", rootPath);
        }
        if (!Objects.equal(pathField, Defaults.PATH_FIELD)) {
            builder.field("path_field", pathField);
        }
        return builder;
    }

    private BytesHolder loadFile(String path) {
        try {
            File file = new File(rootPath, path);
            return new BytesHolder(Streams.copyToByteArray(file));
        } catch (IOException ex) {
            logger.debug("Error retrieving source from file {}", ex, path);
        }
        return null;
    }

    public static class Builder {

        private String rootPath = Defaults.ROOT_PATH;

        private String pathField = Defaults.PATH_FIELD;

        public Builder rootPath(String rootPath) {
            this.rootPath = rootPath;
            return this;
        }

        public Builder pathField(String pathField) {
            this.pathField = pathField;
            return this;
        }

        public FileSourceProvider build() {
            return new FileSourceProvider(rootPath, pathField);
        }

    }

}
