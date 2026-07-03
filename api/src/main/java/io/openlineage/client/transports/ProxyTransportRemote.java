/*
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package io.openlineage.client.transports;

/**
 * Global registry for proxy transport instances.
 *
 * <p>Sets the {@link ProxyTransportControllable} reference when a ProxyTransport is created by
 * OpenLineage's TransportFactory, enabling any code to flush buffered events via {@link #emitAll()}.
 */
public class ProxyTransportRemote {

    private static volatile ProxyTransportControllable controllable;

    /**
     * Registers the current transport instance as the globally accessible controllable.
     * @param c underlying controllable implementation
     **/
    public static void setControllable(ProxyTransportControllable c) {
        controllable = c;
    }

    /** Returns the registered controllable, or null if none has been set. */
    public static ProxyTransportControllable getControllable() {
        return controllable;
    }

    /** Flushes all buffered events for the currently active run. */
    public static void emitAll() {
        ProxyTransportControllable c = getControllable();
        if (c != null) {
            c.emitAll();
        }
    }

    /** Flushes all buffered events for the given run ID. */
    public static void emitAll(String runId) {
        ProxyTransportControllable c = getControllable();
        if (c != null) {
            c.emitAll(runId);
        }
    }

}