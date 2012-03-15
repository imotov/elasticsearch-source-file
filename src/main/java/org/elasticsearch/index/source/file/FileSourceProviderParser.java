package org.elasticsearch.index.source.file;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.source.SourceProvider;
import org.elasticsearch.index.source.SourceProviderParser;

import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeStringValue;


/**
 *
 */
public class FileSourceProviderParser implements SourceProviderParser {

    public final String rootPath;
    public final String pathField;

    @Inject
    public FileSourceProviderParser(@IndexSettings Settings indexSettings, @Assisted String name, @Assisted Settings settings) {
        this.rootPath = settings.get("root_path", FileSourceProvider.Defaults.ROOT_PATH);
        this.pathField = settings.get("path_field", FileSourceProvider.Defaults.PATH_FIELD);
    }

    @Override public SourceProvider parse(Map<String, Object> node) {
        FileSourceProvider.Builder builder = new FileSourceProvider.Builder()
                .pathField(pathField)
                .rootPath(rootPath);

        for (Map.Entry<String, Object> entry : node.entrySet()) {
            String fieldName = Strings.toUnderscoreCase(entry.getKey());
            Object fieldNode = entry.getValue();
            if (fieldName.equals("root_path") && fieldNode != null) {
                builder.rootPath(nodeStringValue(fieldNode, rootPath));
            } else if (fieldName.equals("path_field") && fieldNode != null) {
                builder.pathField(nodeStringValue(fieldNode, pathField));
            }
        }

        return builder.build();
    }
}
