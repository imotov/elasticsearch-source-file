package org.elasticsearch.plugin.source.file;

import org.elasticsearch.plugins.AbstractPlugin;

/**
 *
 */
public class SourceFilePlugin extends AbstractPlugin {

    @Override
    public String name() {
        return "source-file";
    }

    @Override
    public String description() {
        return "File-based source provider";
    }
}
