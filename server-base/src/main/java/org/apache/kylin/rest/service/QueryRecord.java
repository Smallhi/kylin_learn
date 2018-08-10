package org.apache.kylin.rest.service;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.rest.model.Query;

@SuppressWarnings("serial")
class QueryRecord extends RootPersistentEntity {

    @JsonProperty()
    private Query[] queries;

    public QueryRecord() {

    }

    public QueryRecord(Query[] queries) {
        this.queries = queries;
    }

    public Query[] getQueries() {
        return queries;
    }

    public void setQueries(Query[] queries) {
        this.queries = queries;
    }

}
