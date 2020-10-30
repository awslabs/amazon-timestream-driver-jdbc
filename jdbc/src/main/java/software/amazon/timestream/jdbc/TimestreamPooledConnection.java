/*
 * Copyright <2020> Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package software.amazon.timestream.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.StatementEventListener;
import java.sql.Connection;
import java.util.LinkedList;
import java.util.List;

/**
 * A class that is a wrapper around a TimestreamConnection instance which helps to
 * recycle/retrieve the underlying physical connections to/from a connection pool.
 */
public class TimestreamPooledConnection implements javax.sql.PooledConnection {
  private static final Logger LOGGER = LoggerFactory.getLogger(TimestreamPooledConnection.class);
  private final List<ConnectionEventListener> connectionEventListeners = new LinkedList<>();
  private final TimestreamConnection timestreamConnection;

  /**
   * Constructor for TimestreamPooledConnection.
   *
   * @param connection A TimestreamConnection instance.
   */
  TimestreamPooledConnection(TimestreamConnection connection) {
    this.timestreamConnection = connection;
  }

  @Override
  public Connection getConnection() {
    return this.timestreamConnection;
  }

  @Override
  public void close() {
    LOGGER.debug("Notify all connection listeners this PooledConnection object is closed.");
    final ConnectionEvent event = new ConnectionEvent(this, null);
    connectionEventListeners.forEach(l -> l.connectionClosed(event));
  }

  @Override
  public void addConnectionEventListener(ConnectionEventListener listener) {
    LOGGER.debug("Add a ConnectionEventListener to this PooledConnection.");
    connectionEventListeners.add(listener);
  }

  @Override
  public void removeConnectionEventListener(ConnectionEventListener listener) {
    LOGGER.debug("Remove the ConnectionEventListener attached to this PooledConnection.");
    connectionEventListeners.remove(listener);
  }

  @Override
  public void addStatementEventListener(StatementEventListener listener) {
    // Do nothing, statement pooling is not supported.
    LOGGER.debug("addStatementEventListener is called on the current PooledConnection object.");
  }

  @Override
  public void removeStatementEventListener(StatementEventListener listener) {
    // Do nothing, statement pooling is not supported.
    LOGGER.debug("removeStatementEventListener is called on the current PooledConnection object.");
  }
}
