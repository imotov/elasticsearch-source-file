package org.elasticsearch.index.source.file;

import org.elasticsearch.index.source.SourceFilterModule;

/**
 *
 */
public class FileSourceProviderBinderProcessor extends SourceFilterModule.SourceFilterBinderProcessor {
    @Override
    public void processSourceProviders(SourceFilterBindings sourceProviderBindings) {
        sourceProviderBindings.processSourceProvider(FileSourceFilter.Defaults.NAME, FileSourceFilter.class);
    }
}
