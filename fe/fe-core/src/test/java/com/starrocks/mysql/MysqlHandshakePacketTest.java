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

package com.starrocks.mysql;

import com.google.common.primitives.Bytes;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

public class MysqlHandshakePacketTest {
    private byte[] buf;
    private MysqlCapability capability;

    @Mocked
    MysqlPassword mysqlPassword;

    @Before
    public void setUp() {
        buf = new byte[20];
        for (int i = 0; i < 20; ++i) {
            buf[i] = (byte) ('a' + i);
        }

        new Expectations() {
            {
                MysqlPassword.createRandomString();
                minTimes = 0;
                result = buf;
            }
        };

        capability = new MysqlCapability(0);
    }

    @Test
    public void testWrite() {
        MysqlHandshakePacket packet = new MysqlHandshakePacket(1090, false, MysqlPassword.createRandomString());
        MysqlSerializer serializer = MysqlSerializer.newInstance(capability);

        packet.writeTo(serializer);
        ByteBuffer buffer = serializer.toByteBuffer();

        // assert protocol version
        Assert.assertEquals(10, MysqlCodec.readInt1(buffer));
        // server version
        Assert.assertEquals("8.0.33", new String(MysqlCodec.readNulTerminateString(buffer)));
        // connection id
        Assert.assertEquals(1090, MysqlCodec.readInt4(buffer));
        // plugin data 1
        byte[] pluginData1 = MysqlCodec.readFixedString(buffer, 8);
        Assert.assertEquals(0, MysqlCodec.readInt1(buffer));
        int flags = 0;
        flags = MysqlCodec.readInt2(buffer);
        // char set
        Assert.assertEquals(33, MysqlCodec.readInt1(buffer));
        // status flags
        Assert.assertEquals(0, MysqlCodec.readInt2(buffer));
        // capability flags
        flags |= MysqlCodec.readInt2(buffer) << 16;
        Assert.assertEquals(MysqlCapability.DEFAULT_CAPABILITY.getFlags(), flags);
        // length of plugin data
        Assert.assertEquals(21, MysqlCodec.readInt1(buffer));
        // length of plugin data
        byte[] toCheck = new byte[10];
        byte[] reserved = MysqlCodec.readFixedString(buffer, 10);
        for (int i = 0; i < 10; ++i) {
            Assert.assertEquals(toCheck[i], reserved[i]);
        }
        byte[] pluginData2 = MysqlCodec.readFixedString(buffer, 12);
        byte[] pluginData = Bytes.concat(pluginData1, pluginData2);
        for (int i = 0; i < 20; ++i) {
            Assert.assertEquals(buf[i], pluginData[i]);
        }

        // one byte
        Assert.assertEquals(0, MysqlCodec.readInt1(buffer));
        Assert.assertEquals(22, buffer.remaining());
    }

}
