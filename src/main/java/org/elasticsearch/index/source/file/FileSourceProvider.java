package org.elasticsearch.index.source.file;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.elasticsearch.common.base.Objects;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.MergeContext;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.internal.SourceFieldMapper;
import org.elasticsearch.index.source.SourceProvider;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class FileSourceProvider implements SourceProvider {

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

    @Override public String name() {
        return Defaults.NAME;
    }

    @Override public Field parseCreateField(String name, ParseContext context) throws IOException {
        // Extract pathField
        byte[] data = context.source();
        int dataOffset = context.sourceOffset();
        int dataLength = context.sourceLength();
        Tuple<XContentType, Map<String, Object>> mapTuple = XContentHelper.convertToMap(data, dataOffset, dataLength, true);
        Object pathFieldObject = mapTuple.v2().get(pathField);
        if(pathFieldObject != null && pathFieldObject instanceof String) {
            String path = (String) pathFieldObject;
            byte[] pathData = path.getBytes();
            return new Field(name, pathData, 0, pathData.length);
        }
        return null;
    }

    @Override public byte[] extractSource(Document doc) {
        Fieldable sourceField = doc.getFieldable(SourceFieldMapper.NAME);
        if (sourceField != null) {
            byte[] pathData = sourceField.getBinaryValue();
            String path = new String(pathData);
            File file = new File(rootPath, path);
            try {
                return Streams.copyToByteArray(file);
            } catch (IOException ex) {
                //????
            }
        }
        return null;
    }

    @Override public void merge(SourceProvider mergeWith, MergeContext mergeContext) {
        if(mergeWith instanceof FileSourceProvider) {
            FileSourceProvider sourceProviderMergeWith =  (FileSourceProvider) mergeWith;
            // Allow changing rootPath
            if (sourceProviderMergeWith.rootPath != null) {
                this.rootPath = sourceProviderMergeWith.rootPath;
            }
        }

    }

    @Override public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        if (!Objects.equal(rootPath, Defaults.ROOT_PATH)) {
            builder.field("root_path", rootPath);
        }
        if (!Objects.equal(pathField, Defaults.PATH_FIELD)) {
            builder.field("path_field", pathField);
        }
        return builder;
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