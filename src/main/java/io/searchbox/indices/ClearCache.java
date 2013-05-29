package io.searchbox.indices;

import io.searchbox.AbstractAction;
import io.searchbox.AbstractMultiIndexActionBuilder;

/**
 * @author Dogukan Sonmez
 * @author cihat keser
 *
 * TODO complete missing integration tests
 */
public class ClearCache extends AbstractAction {

    private ClearCache() {
    }

    private ClearCache(Builder builder) {
        this.indexName = builder.getJoinedIndices();

        if(builder.filter) {
            this.addParameter("fiter", true);
        }

        if(builder.bloom) {
            this.addParameter("bloom", true);
        }

        if(builder.fieldData) {
            this.addParameter("field_data", true);
        }

        setURI(buildURI());
    }

    @Override
    protected String buildURI() {
        StringBuilder sb = new StringBuilder();
        sb.append(super.buildURI()).append("/_cache/clear");
        return sb.toString();
    }

    @Override
    public String getRestMethodName() {
        return "POST";
    }

    public static class Builder extends AbstractMultiIndexActionBuilder<ClearCache, Builder> {
        private boolean filter;
        private boolean fieldData;
        private boolean bloom;

        public Builder setFilter(boolean filter) {
            this.filter = filter;
            return this;
        }

        public Builder setFieldData(boolean fieldData) {
            this.fieldData = fieldData;
            return this;
        }

        public Builder setBloom(boolean bloom) {
            this.bloom = bloom;
            return this;
        }

        @Override
        public ClearCache build() {
            return new ClearCache(this);
        }
    }
}