package graveldb.datastore.lsmtree;

public record KeyValuePair(String key, String value, boolean isDeleted) {}
