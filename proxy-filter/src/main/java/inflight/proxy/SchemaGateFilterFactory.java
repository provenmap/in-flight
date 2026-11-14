package inflight.proxy;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;

/** Creates a {@link SchemaGateFilter} per connection, all sharing one registry lookup and its cache, and the mode. */
@Plugin(configType = SchemaGateConfig.class)
public class SchemaGateFilterFactory implements FilterFactory<SchemaGateConfig, SchemaGateFilterFactory.Gate> {

    /** What every connection's filter shares: the lookup, and whether it only reports. */
    public record Gate(SchemaLookup lookup, boolean reportOnly) {}

    public SchemaGateFilterFactory() {
    }

    @Override
    public Gate initialize(FilterFactoryContext context, SchemaGateConfig config) {
        SchemaGateConfig c = config == null ? new SchemaGateConfig(null, null, (Long) null, (Boolean) null) : config;
        return new Gate(new SchemaLookup.Apicurio(c), c.reportOnly());
    }

    @Override
    public Filter createFilter(FilterFactoryContext context, Gate gate) {
        return new SchemaGateFilter(gate.lookup(), context.filterDispatchExecutor(), gate.reportOnly());
    }
}
