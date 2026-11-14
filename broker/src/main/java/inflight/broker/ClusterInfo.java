package inflight.broker;

/**
 * What a connection test reports: the cluster's id (or name), how many nodes answer, and how many streams it holds.
 */
public record ClusterInfo(String id, int nodes, int streams) {}
