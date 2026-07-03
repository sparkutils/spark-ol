package io.openlineage.client.transports;

/**
 * Contract for controlling a proxy transport instance's event emission.
 *
 * <p>Implemented by {@code ProxyTransport} and registered via {@link ProxyTransportRemote},
 * allowing any code to flush buffered events either for the active run or a specific run ID.
 */
public interface ProxyTransportControllable {

    /** Flushes all buffered events for the currently active run. */
    void emitAll();

    /**
     * Flushes all buffered events for the specified run ID.
     * @param runId spark run id
     * */
    void emitAll(String runId);
}