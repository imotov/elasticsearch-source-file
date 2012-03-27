package org.elasticsearch.index.source.file;

import org.elasticsearch.common.BytesHolder;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.source.SourceFilter;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import static java.util.Collections.emptyMap;

/**
 *
 */
public class FileSourceFilter implements SourceFilter {

    protected final ESLogger logger = Loggers.getLogger(getClass());

    public static class Defaults {
        public static final String NAME = "file";
        public static final String ROOT_PATH = "/";
        public static final String PATH_FIELD = "path";
    }

    private String rootPath;

    private final String pathField;

    private final String name;


    @Inject
    public FileSourceFilter(@Assisted String name, @Assisted Settings settings) {
        this.name = name;
        this.rootPath = settings.get("root_path", Defaults.ROOT_PATH);
        this.pathField = settings.get("path_field", Defaults.PATH_FIELD);
    }
    @Override
    public String name() {
        return name;
    }

    @Override
    public Map<String, Object> dehydrateSource(String type, String id, Map<String, Object> source) throws IOException {
        Object pathFieldObject = source.get(pathField);
        if (pathFieldObject != null && pathFieldObject instanceof String) {
            // Replace path with just the portion that is needed to restore source in the future
            Map<String, Object> dehydratedMap = Maps.newHashMap();
            dehydratedMap.put(pathField, pathFieldObject);
            return dehydratedMap;
        } else {
            // Path field is not found - don't store source at all
            return emptyMap();
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

    private BytesHolder loadFile(String path) {
        try {
            File file = new File(rootPath, path);
            return new BytesHolder(Streams.copyToByteArray(file));
        } catch (IOException ex) {
            logger.debug("Error retrieving source from file {}", ex, path);
        }
        return null;
    }

}
