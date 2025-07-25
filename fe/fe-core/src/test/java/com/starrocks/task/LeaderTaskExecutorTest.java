// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.task;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeaderTaskExecutorTest {
    private static final Logger LOG = LoggerFactory.getLogger(LeaderTaskExecutorTest.class);
    private static final int THREAD_NUM = 1;
    private static final long SLEEP_MS = 10L;

    private LeaderTaskExecutor executor;

    @BeforeEach
    public void setUp() {
        executor = new LeaderTaskExecutor("master_task_executor_test", THREAD_NUM, false);
        executor.start();
    }

    @AfterEach
    public void tearDown() {
        if (executor != null) {
            executor.close();
        }
    }

    @Test
    public void testSubmit() {
        // submit task
        LeaderTask task1 = new TestLeaderTask(1L);
        Assertions.assertTrue(executor.submit(task1));
        Assertions.assertEquals(1, executor.getTaskNum());
        // submit same running task error
        Assertions.assertFalse(executor.submit(task1));
        Assertions.assertEquals(1, executor.getTaskNum());

        // submit another task
        LeaderTask task2 = new TestLeaderTask(2L);
        Assertions.assertTrue(executor.submit(task2));
        Assertions.assertEquals(2, executor.getTaskNum());

        // wait for tasks run to end
        try {
            // checker thread interval is 1s
            // sleep 3s
            Thread.sleep(SLEEP_MS * 300);
            Assertions.assertEquals(0, executor.getTaskNum());
        } catch (InterruptedException e) {
            LOG.error("error", e);
        }
    }

    @Test
    public void testPoolSize() {
        int size = executor.getCorePoolSize();
        executor.setPoolSize(size + 1);
        Assertions.assertEquals(size + 1, executor.getCorePoolSize());
        executor.setPoolSize(size);
        Assertions.assertEquals(size, executor.getCorePoolSize());
    }

    private class TestLeaderTask extends LeaderTask {

        public TestLeaderTask(long signature) {
            this.signature = signature;
        }

        @Override
        protected void exec() {
            LOG.info("run exec. signature: {}", signature);
            try {
                Thread.sleep(SLEEP_MS);
            } catch (InterruptedException e) {
                LOG.error("error", e);
            }
        }

    }
}
