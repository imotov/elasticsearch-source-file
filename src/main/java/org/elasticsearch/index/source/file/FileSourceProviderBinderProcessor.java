package org.elasticsearch.index.source.file;

import org.elasticsearch.index.source.SourceProviderModule;

/**
 *
 */
public class FileSourceProviderBinderProcessor extends SourceProviderModule.SourceProviderBinderProcessor {
    @Override
    public void processSourceProviders(SourceProviderBindings sourceProviderBindings) {
        sourceProviderBindings.processSourceProvider("file", FileSourceProviderParser.class);
    }
}
