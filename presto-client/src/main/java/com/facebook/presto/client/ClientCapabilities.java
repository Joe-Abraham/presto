/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.client;

/**
 * Capabilities that a client may advertise to the server via the
 * {@code X-Presto-Client-Capabilities} header.  The server uses these to
 * decide which features and response formats are safe to use for a given
 * client connection.
 */
public enum ClientCapabilities
{
    /**
     * The client understands parametric TIMESTAMP types with arbitrary
     * precision (0–12), including {@code LongTimestamp} values for
     * precisions 7–12 that are represented as a pair of (epochMicros,
     * picosOfMicro).
     */
    PARAMETRIC_DATETIME
}
