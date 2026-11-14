package inflight.proxy;

import io.kroxylicious.proxy.filter.Filter;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;

/** Creates an {@link IntentAliasFilter} per connection. Use as {@code type: inflight.proxy.IntentAliasFilterFactory}. */
@Plugin(configType = IntentAliasConfig.class)
public class IntentAliasFilterFactory implements FilterFactory<IntentAliasConfig, IntentAliasConfig> {

    public IntentAliasFilterFactory() {
    }

    @Override
    public IntentAliasConfig initialize(FilterFactoryContext context, IntentAliasConfig config) {
        return config == null ? IntentAliasConfig.DEFAULT : config;
    }

    @Override
    public Filter createFilter(FilterFactoryContext context, IntentAliasConfig config) {
        return new IntentAliasFilter(config == null ? IntentAliasConfig.DEFAULT : config);
    }
}
