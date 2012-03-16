package org.elasticsearch.plugin.source.file;

import org.elasticsearch.index.source.SourceProviderModule;
import org.elasticsearch.index.source.file.FileSourceProviderBinderProcessor;
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

    public void onModule(SourceProviderModule sourceProviderModule) {
        sourceProviderModule.addProcessor(new FileSourceProviderBinderProcessor());
    }
}
