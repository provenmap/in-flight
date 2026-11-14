package inflight.broker;

/**
 * What an adapter can do. Each one is a promise the rest of the tool relies on, so declare one only when the broker
 * keeps it; a missing capability turns a feature off, a false one gives wrong numbers.
 */
public enum Capability {

    /**
     * Messages can be read without side effects: reading takes nothing away from anyone, changes no group's position,
     * and does not mark anything redelivered. Kafka, Pulsar (a non-durable reader) and RabbitMQ streams can; a classic
     * RabbitMQ or SQS queue cannot, because reading a message takes it from the service that needed it. The observer,
     * discovery's sampling, the intent tail and rehearse all need this.
     */
    OBSERVE,

    /**
     * Each group's committed position per shard can be read, and offsets are dense, so {@code end - committed} is how
     * many messages a group has not dealt with. The headline's "in lag" figure needs this.
     */
    GROUP_LAG,

    /**
     * The tool may read a dead-letter stream under a group of its own, commit how far it got (with a note), and write
     * messages back to their source streams. Redrive needs this, and the profile must also allow writes.
     */
    REDRIVE
}
