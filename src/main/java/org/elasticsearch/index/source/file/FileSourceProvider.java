package org.elasticsearch.index.source.file;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.elasticsearch.common.base.Objects;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.CachedStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.index.mapper.MergeContext;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.internal.SourceFieldMapper;
import org.elasticsearch.index.source.SourceProvider;

import java.io.File;
import java.io.IOException;
import java.util.Date;
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
        public static final String BODY_FIELD = "body";
        public static final String FILE_DATE_FIELD = "date";
        public static final String FILE_ENCODING = "UTF-8";
        public static final String FILE_ENCODING_FIELD = "encoding";
    }

    private String rootPath;

    private final String pathField;

    private final String bodyField;

    private final String encoding;

    private final String encodingField;

    private final String fileDateField;

    protected FileSourceProvider(String rootPath, String pathField, String bodyField,
                                 String encoding, String encodingField, String fileDateField) {

        this.rootPath = rootPath;
        this.pathField = pathField;
        this.bodyField = bodyField;
        this.encoding = encoding;
        this.encodingField = encodingField;
        this.fileDateField = fileDateField;
    }

    @Override
    public String name() {
        return Defaults.NAME;
    }

    @Override
    public Field parseCreateField(String name, ParseContext context) throws IOException {
        byte[] data = context.source();
        int dataOffset = context.sourceOffset();
        int dataLength = context.sourceLength();
        Tuple<XContentType, Map<String, Object>> mapTuple = XContentHelper.convertToMap(data, dataOffset, dataLength, true);
        Map<String, Object> sourceMap = mapTuple.v2();
        Map<String, Object> expandedMap = expandSource(sourceMap);
        if (sourceMap != expandedMap) {
            // Source was expanded - replace existing source for parsing
            CachedStreamOutput.Entry cachedEntry = CachedStreamOutput.popEntry();
            StreamOutput streamOutput;
            streamOutput = cachedEntry.cachedBytes();
            XContentType contentType = mapTuple.v1();
            XContentBuilder builder = XContentFactory.contentBuilder(contentType, streamOutput).map(expandedMap);
            builder.close();
            // Update source with an expanded version
            context.source(cachedEntry.bytes().copiedByteArray(), 0, cachedEntry.bytes().size());
            CachedStreamOutput.pushEntry(cachedEntry);
        }
        // Store original source in the source field
        return new Field(name, data, dataOffset, dataLength);
    }

    @Override
    public byte[] extractSource(Document doc) {
        Fieldable sourceField = doc.getFieldable(SourceFieldMapper.NAME);
        if (sourceField != null) {
            Tuple<XContentType, Map<String, Object>> mapTuple = XContentHelper.convertToMap(
                    sourceField.getBinaryValue(), sourceField.getBinaryOffset(), sourceField.getBinaryLength(), true);
            XContentType contentType = mapTuple.v1();
            Map<String, Object> sourceMap = mapTuple.v2();
            try {
                Map<String, Object> expandedMap = expandSource(sourceMap);
                if (sourceMap != expandedMap) {
                    XContentBuilder builder = XContentFactory.contentBuilder(contentType).map(expandedMap);
                    return builder.copiedBytes();
                } else {
                    return sourceField.getBinaryValue();
                }
            } catch (IOException ex) {
                logger.warn("Source not found", ex);
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

    private Map<String, Object> expandSource(Map<String, Object> sourceMap) throws IOException {
        Object pathFieldObject = sourceMap.get(pathField);
        if (pathFieldObject != null && pathFieldObject instanceof String) {
            String fileEncoding = encoding;
            Object encodingFieldObject = sourceMap.get(encodingField);
            if (encodingFieldObject != null && encodingFieldObject instanceof String) {
                fileEncoding = (String) encodingFieldObject;
            }
            String path = (String) pathFieldObject;
            File file = new File(rootPath, path);
            byte[] fileData = Streams.copyToByteArray(file);
            Map<String, Object> expandedSourceMap = Maps.newHashMap();
            expandedSourceMap.putAll(sourceMap);
            expandedSourceMap.put(bodyField, new String(fileData, fileEncoding));
            expandedSourceMap.put(fileDateField, new Date(file.lastModified()));
            return expandedSourceMap;
        } else {
            return sourceMap;
        }

    }

    private byte[] readFile(String path) throws IOException {
        File file = new File(rootPath, path);
        return Streams.copyToByteArray(file);
    }


    public static class Builder {

        private String rootPath = Defaults.ROOT_PATH;

        private String pathField = Defaults.PATH_FIELD;

        private String bodyField = Defaults.BODY_FIELD;

        private String encoding = Defaults.FILE_ENCODING;

        private String encodingField = Defaults.FILE_ENCODING_FIELD;

        private String fileDateField = Defaults.FILE_DATE_FIELD;


        public Builder rootPath(String rootPath) {
            this.rootPath = rootPath;
            return this;
        }

        public Builder pathField(String pathField) {
            this.pathField = pathField;
            return this;
        }

        public Builder bodyField(String bodyField) {
            this.bodyField = bodyField;
            return this;
        }

        public Builder encoding(String encoding) {
            this.encoding = encoding;
            return this;
        }

        public Builder encodingField(String encodingField) {
            this.encodingField = encodingField;
            return this;
        }

        public Builder fileDateField(String fileDateField) {
            this.fileDateField = fileDateField;
            return this;
        }

        public FileSourceProvider build() {
            return new FileSourceProvider(rootPath, pathField, bodyField, encoding, encodingField, fileDateField);
        }

    }

}
