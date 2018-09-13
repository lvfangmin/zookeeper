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

package org.apache.zookeeper.server.quorum;

import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.PortAssignment;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.server.DataTree;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.server.persistence.FileTxnLog;
import org.apache.zookeeper.server.persistence.TxnLog;
import org.apache.zookeeper.server.util.DigestCalculator;
import org.apache.zookeeper.server.ServerMetrics;
import org.apache.zookeeper.test.ClientBase;
import org.apache.zookeeper.txn.TxnDigest;
import org.apache.zookeeper.txn.TxnHeader;
import org.apache.zookeeper.server.DataTree.ProcessTxnResult;
import org.apache.zookeeper.server.TxnLogDigestTest;
import org.junit.Test;
import org.apache.zookeeper.ZooKeeper.States;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.jute.Record;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.zookeeper.test.ClientBase.CONNECTION_TIMEOUT;

public class QuorumDigestTest extends ClientBase {

    private static final Logger LOG = 
          LoggerFactory.getLogger(QuorumDigestTest.class);

    private QuorumPeerMainTest.Servers servers;

    @BeforeClass
    public static void applyMockUps() {
        new DataTreeMock();
    }

    @Before
    public void setup() throws Exception {
        DigestCalculator.setDigestEnabled(true);
        ServerMetrics.DIGEST_MISMATCHES_COUNT.reset();
        servers = QuorumPeerMainTest.LaunchServers(3, 1, null);
    }

    @After
    public void tearDown() throws Exception {
        if (servers != null) {
            servers.shutDownAllServers();
        }
        DigestCalculator.setDigestEnabled(false);
        System.clearProperty(LearnerHandler.FORCE_SNAP_SYNC);
        DataTreeMock.reset();
    }

    /**
     * Check positive case without digest mismatch during diff sync.
     */
    @Test
    public void testDigestMatchesDuringDiffSync() throws Exception {
        triggerSync(false);
    }

    /**
     * Check positive case without digest mismatch during snap sync.
     */
    @Test
    public void testDigestMatchesDuringSnapSync() throws Exception {
        triggerSync(true);

        // have some extra txns
        int leader = servers.findLeader();
        TxnLogDigestTest.performOperations(servers.zk[leader], 
                "/testDigestMatchesDuringSnapSync");
        Assert.assertEquals(0L, getMismatchDigestCount());
    }

    @Test
    public void testDigestMatchesWithAsyncRequests() throws Exception {

        int leader = servers.findLeader();

        final ZooKeeper client = servers.zk[leader];
        final AtomicBoolean stopped = new AtomicBoolean(true);
        final String prefix = "/testDigestMatchesWithAsyncRequests";

        // start a thread to send requests asynchronously,
        Thread createTrafficThread = new Thread () {
            @Override
            public void run() {
                int i = 0;
                while (!stopped.get()) {
                    String path = prefix + "-" + i;
                    client.create(path, path.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, 
                            CreateMode.PERSISTENT, new StringCallback() {
                        @Override
                        public void processResult(int rc, String path, 
                                Object ctx, String name) {
                            // ignore the result 
                        }
                    }, null);
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) { /* ignore */ }
                }
            }
        };
        createTrafficThread.start();

        // shutdown a follower and observer
        List<Integer> targets = Arrays.asList(
                servers.findAnyFollower(), servers.findAnyObserver());
        stopServers(targets);

        // start the follower and observer to have a diff sync
        startServers(targets);

        // make sure there is no digest mismatch
        Assert.assertEquals(0L, getMismatchDigestCount());

        // stop the leader
        targets = Arrays.asList(leader);
        stopServers(targets);
        startServers(targets);

        // make sure there is no digest mismatch
        Assert.assertEquals(0L, getMismatchDigestCount());

        stopped.set(true);
    }

    /**
     * Check negative case by injecting txn miss during syncing.
     */
    @Test
    public void testDigestMismatchesWhenTxnLost() throws Exception {
        // make sure there is no mismatch after all servers start up
        Assert.assertEquals(0L, getMismatchDigestCount());

        // shutdown a follower and observer
        List<Integer> targets = Arrays.asList(
                servers.findAnyFollower(), servers.findAnyObserver());
        stopServers(targets);

        int leader = servers.findLeader();
        triggerOps(leader, "/p1");

        Assert.assertEquals(0L, getMismatchDigestCount());

        DataTreeMock.skipTxnZxid = "100000006";

        // start the follower and observer to have a diff sync
        startServers(targets);

        long misMatchCount = getMismatchDigestCount();
        Assert.assertNotEquals(0L, misMatchCount);

        triggerOps(leader, "/p2");
        Assert.assertNotEquals(misMatchCount, getMismatchDigestCount());
    }

    private void stopServers(List<Integer> sids) throws InterruptedException {
        for (int sid : sids) {
            servers.mt[sid].shutdown();
            QuorumPeerMainTest.waitForOne(servers.zk[sid], States.CONNECTING);
        }
    }

    private void startServers(List<Integer> sids) throws InterruptedException {
        for (int sid : sids) {
            servers.mt[sid].start();
            QuorumPeerMainTest.waitForOne(servers.zk[sid], States.CONNECTED);
        }
    }

    private void triggerOps(int sid, String prefix) throws Exception {
        TxnLogDigestTest.performOperations(servers.zk[sid], prefix);
        servers.restartClient(sid, null);
        QuorumPeerMainTest.waitForOne(servers.zk[sid], States.CONNECTED);
    }

    private void triggerSync(boolean snapSync) throws Exception {
        if (snapSync) {
            System.setProperty(LearnerHandler.FORCE_SNAP_SYNC, "true");
        }

        // make sure there is no mismatch after all servers start up
        Assert.assertEquals(0L, getMismatchDigestCount());

        int leader = servers.findLeader();
        triggerOps(leader, "/p1");

        Assert.assertEquals(0L, getMismatchDigestCount());

        // shutdown a follower and observer
        List<Integer> targets = Arrays.asList(
                servers.findAnyFollower(), servers.findAnyObserver());
        stopServers(targets);

        // do some extra writes
        triggerOps(leader, "/p2");

        // start the follower and observer to have a diff sync
        startServers(targets);

        Assert.assertEquals(0L, getMismatchDigestCount());
    }

    public static long getMismatchDigestCount() {
        return ServerMetrics.DIGEST_MISMATCHES_COUNT.getValues()
                            .get("digest_mismatches_count");
    }

    public static final class DataTreeMock extends MockUp<DataTree> {

        static String skipTxnZxid = ""; 

        @Mock
        public ProcessTxnResult processTxn(Invocation invocation, 
                TxnHeader header, Record txn, TxnDigest digest) {
            if (header != null && Long.toHexString(header.getZxid()).equals(skipTxnZxid)) {
                LOG.info("skip process txn {}", header.getZxid());
                ProcessTxnResult rc = new ProcessTxnResult();
                rc.path = "";
                rc.stat = new Stat();
                rc.multiResult = new ArrayList<ProcessTxnResult>();
                return rc;
            }
            return invocation.proceed(header, txn, digest);
        }

        public static void reset() {
            skipTxnZxid = ""; 
        }
    }
}
