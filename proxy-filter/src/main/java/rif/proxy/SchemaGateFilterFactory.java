package rif.proxy;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;

/** Creates a {@link SchemaGateFilter} per connection, all sharing one registry lookup and its cache. */
@Plugin(configType = SchemaGateConfig.class)
public class SchemaGateFilterFactory implements FilterFactory<SchemaGateConfig, SchemaLookup> {

    public SchemaGateFilterFactory() {
    }

    @Override
    public SchemaLookup initialize(FilterFactoryContext context, SchemaGateConfig config) {
        return new SchemaLookup.Apicurio(config == null ? new SchemaGateConfig(null, null, (Long) null) : config);
    }

    @Override
    public Filter createFilter(FilterFactoryContext context, SchemaLookup lookup) {
        return new SchemaGateFilter(lookup, context.filterDispatchExecutor());
    }
}
