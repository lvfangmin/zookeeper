/*
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

import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.StatPersisted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

/**
 * Using CRC to calculate the digest for the given data.
 */
public class CRC32DigestCalculator extends AbstractDigestCalculator {

    private static final Logger LOG = LoggerFactory.getLogger(
            CRC32DigestCalculator.class);

    /** 
     * Besides the path and data, the following stat fields are included in 
     * the digest calculation:
     *
     * - long czxid    8 bytes
     * - long mzxid    8 bytes
     * - long pzxid    8 bytes
     * - long ctime    8 bytes
     * - long mtime    8 bytes
     * - int version   4 bytes
     * - int cversion  4 bytes
     * - int aversion  4 bytes
     * - long ephemeralOwner 8 bytes
     *
     * Specific aliase like "/" and nodes under "/zookeeper/" are ignored 
     * for now.
     */
    @Override
    public long calculateDigest(String path, byte[] data, StatPersisted stat) {
        if(!digestEnabled()) {
            return 0;
        }

        // Quota nodes are updated locally, there is inconsistent issue 
        // when we tried to release digest feature at the beginning. 
        //
        // Instead of taking time to fix that, we decided to disable digest
        // check for all the nodes under /zookeeper/ first. 
        //
        // We can enable this after fixing that inconsistent problem. The 
        // digest version in the protocol enables us to change the digest 
        // calculation without disrupting the system.
        if (path.startsWith(ZooDefs.ZOOKEEPER_NODE_SUBTREE)) {
            return 0;
        }

        LOG.debug("Calculating digest for path {}, data {}, stat {}.", 
                path, data, stat);

        // total = 8 * 6 + 4 * 3 = 60 bytes
        byte b[] = new byte[60];
        ByteBuffer bb = ByteBuffer.wrap(b);
        bb.putLong(stat.getCzxid());
        bb.putLong(stat.getMzxid());
        bb.putLong(stat.getPzxid());
        bb.putLong(stat.getCtime());
        bb.putLong(stat.getMtime());
        bb.putInt(stat.getVersion());
        bb.putInt(stat.getCversion());
        bb.putInt(stat.getAversion());
        bb.putLong(stat.getEphemeralOwner());

        CRC32 crc = new CRC32();
        crc.update(path.getBytes());
        if (data != null) {
            crc.update(data);
        }
        crc.update(b);
        return crc.getValue();
    }
}
