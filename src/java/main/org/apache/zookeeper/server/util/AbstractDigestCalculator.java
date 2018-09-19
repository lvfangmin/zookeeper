/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server.util;

import org.apache.zookeeper.data.StatPersisted;
import org.apache.zookeeper.server.DataNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The abstract class defined for calculating digest. It defines some common
 * things like the digets version and cache, etc.
 */
public abstract class AbstractDigestCalculator {

    private static final Logger LOG = LoggerFactory.getLogger(
            AbstractDigestCalculator.class);

    // The hardcoded digest version, should bump up this version whenever 
    // we changed the digest method or fields.
    private static final Integer DIGEST_VERSION = 1;

    public static final String ZOOKEEPER_DIGEST_ENABLED = "zookeeper.digest.enabled";
    private boolean digestEnabled = false;

    public AbstractDigestCalculator() {
        digestEnabled = Boolean.getBoolean(ZOOKEEPER_DIGEST_ENABLED);
    }

    /**
     * Return the digest version we're using to calculate the digest.
     */
    public static int getDigestVersion() {
        return DIGEST_VERSION; 
    }

    /**
     * Calculate the digest based on the given params.
     *
     * @param path the path of the node 
     * @param data the data of the node
     * @param stat the stat associated with the node
     * @return the digest calculated from the given params
     */
    public abstract long calculateDigest(String path, byte[] data, 
            StatPersisted stat);

    /**
     * Calculate the digest based on the given path ande data node.
     */
    public long calculateDigest(String path, DataNode node) {
        // This is to have a consistent digest value for "" and "/", the
        // two paths pointing to root. Not going to calculate the digest
        // for aliases.
        //
        // Have to do it here before calling the calculateDigest implemented
        // in sub classes, because during deserialize we'll put the same node
        // again which was representing "".
        if ("/".equals(path)) {
            return 0;
        }

        if (!node.isDigestCached()) {
            long digest = this.calculateDigest(path, node.getData(), node.stat);
            node.setDigest(digest);
            node.setDigestCached(true);
        }
        return node.getDigest();
    }

    /**
     * Return true if the digest is enabled.
     */
    public boolean digestEnabled() {
        return digestEnabled;
    }
}
