// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.server;

import com.google.common.collect.Lists;
import com.google.gson.stream.JsonReader;
import com.staros.proto.FileStoreInfo;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.catalog.Type;
import com.starrocks.common.AlreadyExistsException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReportException;
import com.starrocks.common.InvalidConfException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.connector.share.credential.CloudConfigurationConstants;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.aws.AwsCloudConfiguration;
import com.starrocks.lake.LakeTable;
import com.starrocks.lake.LakeTablet;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.SetDefaultStorageVolumeLog;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockReaderV2;
import com.starrocks.storagevolume.StorageVolume;
import com.starrocks.thrift.TStorageMedium;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_ACCESS_KEY;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_ENDPOINT;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_REGION;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_SECRET_KEY;
import static com.starrocks.connector.share.credential.CloudConfigurationConstants.AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR;

public class SharedDataStorageVolumeMgrTest {
    @Mocked
    private StarOSAgent starOSAgent;

    @Mocked
    private EditLog editLog;

    @Before
    public void setUp() {
        Config.cloud_native_storage_type = "S3";
        Config.aws_s3_access_key = "access_key";
        Config.aws_s3_secret_key = "secret_key";
        Config.aws_s3_region = "region";
        Config.aws_s3_endpoint = "endpoint";
        Config.aws_s3_path = "default-bucket/1";
        Config.enable_load_volume_from_conf = true;

        new MockUp<GlobalStateMgr>() {
            @Mock
            public StarOSAgent getStarOSAgent() {
                return starOSAgent;
            }
        };

        new MockUp<StarOSAgent>() {
            Map<String, FileStoreInfo> fileStores = new HashMap<>();
            private long id = 1;

            @Mock
            public String addFileStore(FileStoreInfo fsInfo) {
                if (fsInfo.getFsKey().isEmpty()) {
                    fsInfo = fsInfo.toBuilder().setFsKey(String.valueOf(id++)).build();
                }
                fileStores.put(fsInfo.getFsKey(), fsInfo);
                return fsInfo.getFsKey();
            }

            @Mock
            public void removeFileStoreByName(String fsName) throws DdlException {
                FileStoreInfo fsInfo = getFileStoreByName(fsName);
                if (fsInfo == null) {
                    throw new DdlException("Failed to remove file store");
                }
                fileStores.remove(fsInfo.getFsKey());
            }

            @Mock
            public FileStoreInfo getFileStoreByName(String fsName) {
                for (FileStoreInfo fsInfo : fileStores.values()) {
                    if (fsInfo.getFsName().equals(fsName)) {
                        return fsInfo;
                    }
                }
                return null;
            }

            @Mock
            public FileStoreInfo getFileStore(String fsKey) {
                return fileStores.get(fsKey);
            }

            @Mock
            public void updateFileStore(FileStoreInfo fsInfo) {
                FileStoreInfo fileStoreInfo = fileStores.get(fsInfo.getFsKey());
                fileStores.put(fsInfo.getFsKey(), fsInfo);
            }
        };

        new MockUp<GlobalStateMgr>() {
            @Mock
            public EditLog getEditLog() {
                return editLog;
            }
        };
    }

    @After
    public void tearDown() {
        Config.cloud_native_storage_type = "S3";
        Config.aws_s3_access_key = "";
        Config.aws_s3_secret_key = "";
        Config.aws_s3_region = "";
        Config.aws_s3_endpoint = "";
        Config.aws_s3_path = "";
        Config.enable_load_volume_from_conf = false;
    }

    @Test
    public void testStorageVolumeCRUD() throws AlreadyExistsException, DdlException, MetaNotFoundException {
        new Expectations() {
            {
                editLog.logSetDefaultStorageVolume((SetDefaultStorageVolumeLog) any);
            }
        };

        String svName = "test";
        String svName1 = "test1";
        // create
        StorageVolumeMgr svm = new SharedDataStorageVolumeMgr();
        List<String> locations = Arrays.asList("s3://abc");
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put("aaa", "bbb");
        storageParams.put(AWS_S3_REGION, "region");
        Assert.assertThrows(DdlException.class,
                () -> svm.createStorageVolume(svName, "S3", locations, storageParams, Optional.empty(), ""));
        storageParams.remove("aaa");
        storageParams.put(AWS_S3_ENDPOINT, "endpoint");
        storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
        String svKey = svm.createStorageVolume(svName, "S3", locations, storageParams, Optional.empty(), "");
        Assert.assertEquals(true, svm.exists(svName));
        Assert.assertEquals(svName, svm.getStorageVolumeName(svKey));
        StorageVolume sv = svm.getStorageVolumeByName(svName);
        CloudConfiguration cloudConfiguration = sv.getCloudConfiguration();
        Assert.assertEquals("region", ((AwsCloudConfiguration) cloudConfiguration).getAwsCloudCredential()
                .getRegion());
        Assert.assertEquals("endpoint", ((AwsCloudConfiguration) cloudConfiguration).getAwsCloudCredential()
                .getEndpoint());
        StorageVolume sv1 = svm.getStorageVolume(sv.getId());
        Assert.assertEquals(sv1.getId(), sv.getId());
        try {
            svm.createStorageVolume(svName, "S3", locations, storageParams, Optional.empty(), "");
            Assert.fail();
        } catch (AlreadyExistsException e) {
            Assert.assertTrue(e.getMessage().contains("Storage volume 'test' already exists"));
        }

        // update
        storageParams.put(AWS_S3_REGION, "region1");
        storageParams.put(AWS_S3_ENDPOINT, "endpoint1");
        storageParams.put(AWS_S3_ACCESS_KEY, "ak");
        storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
        try {
            svm.updateStorageVolume(svName1, null, null, storageParams, Optional.of(false), "test update");
            Assert.fail();
        } catch (IllegalStateException e) {
            Assert.assertTrue(e.getMessage().contains("Storage volume 'test1' does not exist"));
        }
        storageParams.put("aaa", "bbb");
        Assert.assertThrows(DdlException.class, () ->
                svm.updateStorageVolume(svName, null, null, storageParams, Optional.of(true), "test update"));
        storageParams.remove("aaa");
        svm.updateStorageVolume(svName, null, null, storageParams, Optional.of(true), "test update");
        sv = svm.getStorageVolumeByName(svName);
        cloudConfiguration = sv.getCloudConfiguration();
        Assert.assertEquals("region1", ((AwsCloudConfiguration) cloudConfiguration).getAwsCloudCredential()
                .getRegion());
        Assert.assertEquals("endpoint1", ((AwsCloudConfiguration) cloudConfiguration).getAwsCloudCredential()
                .getEndpoint());
        Assert.assertEquals("test update", sv.getComment());
        Assert.assertEquals(true, sv.getEnabled());

        // set default storage volume
        try {
            svm.setDefaultStorageVolume(svName1);
            Assert.fail();
        } catch (IllegalStateException e) {
            Assert.assertTrue(e.getMessage().contains("Storage volume 'test1' does not exist"));
        }
        svm.setDefaultStorageVolume(svName);
        Assert.assertEquals(sv.getId(), svm.getDefaultStorageVolumeId());

        Throwable ex = Assert.assertThrows(IllegalStateException.class,
                () -> svm.updateStorageVolume(svName, null, null, storageParams, Optional.of(false), ""));
        Assert.assertEquals("Default volume can not be disabled", ex.getMessage());

        // remove
        try {
            svm.removeStorageVolume(svName);
            Assert.fail();
        } catch (IllegalStateException e) {
            Assert.assertTrue(e.getMessage().contains("default storage volume can not be removed"));
        }

        ex = Assert.assertThrows(MetaNotFoundException.class, () -> svm.removeStorageVolume(svName1));
        Assert.assertEquals("Storage volume 'test1' does not exist", ex.getMessage());

        svm.createStorageVolume(svName1, "S3", locations, storageParams, Optional.of(false), "");
        svm.updateStorageVolume(svName1, null, null, storageParams, Optional.of(true), "test update");
        svm.setDefaultStorageVolume(svName1);

        sv = svm.getStorageVolumeByName(svName);
        svm.removeStorageVolume(svName);
        Assert.assertFalse(svm.exists(svName));
    }

    @Test
    public void testImmutableProperties() throws DdlException, AlreadyExistsException {
        String svName = "test";
        StorageVolumeMgr svm = new SharedDataStorageVolumeMgr();
        List<String> locations = List.of("s3://abc");
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put(AWS_S3_REGION, "region");
        storageParams.put(AWS_S3_ENDPOINT, "endpoint");
        storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
        String svKey = svm.createStorageVolume(svName, "S3", locations, storageParams, Optional.empty(), "");
        Assert.assertTrue(svm.exists(svName));

        {
            Map<String, String> modifyParams = new HashMap<>();
            modifyParams.put(CloudConfigurationConstants.AWS_S3_ENABLE_PARTITIONED_PREFIX, "true");
            Assert.assertThrows(DdlException.class, () ->
                    svm.updateStorageVolume(svName, null, null, modifyParams, Optional.of(false), ""));
        }

        {
            Map<String, String> modifyParams = new HashMap<>();
            modifyParams.put(CloudConfigurationConstants.AWS_S3_NUM_PARTITIONED_PREFIX, "12");
            Assert.assertThrows(DdlException.class, () ->
                    svm.updateStorageVolume(svName, null, null, modifyParams, Optional.of(false), ""));
        }
    }

    @Test
    public void testBindAndUnbind() throws DdlException, AlreadyExistsException, MetaNotFoundException {
        String svName = "test";
        StorageVolumeMgr svm = new SharedDataStorageVolumeMgr();
        List<String> locations = Arrays.asList("s3://abc");
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put(AWS_S3_REGION, "region");
        storageParams.put(AWS_S3_ENDPOINT, "endpoint");
        storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
        svm.createStorageVolume(svName, "S3", locations, storageParams, Optional.empty(), "");

        Assert.assertThrows(DdlException.class, () -> svm.bindDbToStorageVolume("0", 1L));
        Assert.assertThrows(DdlException.class, () -> svm.bindTableToStorageVolume("0", 1L, 1L));
        // bind/unbind db and table to storage volume
        Assert.assertTrue(svm.bindDbToStorageVolume(svName, 1L));
        Assert.assertTrue(svm.bindTableToStorageVolume(svName, 1L, 1L));

        svm.updateStorageVolume(svName, null, null, storageParams, Optional.of(false), "test update");
        // disabled storage volume can not be bound.
        Throwable ex = Assert.assertThrows(DdlException.class, () -> svm.bindDbToStorageVolume(svName, 1L));
        Assert.assertEquals(String.format("Storage volume %s is disabled", svName), ex.getMessage());
        ex = Assert.assertThrows(DdlException.class, () -> svm.bindTableToStorageVolume(svName, 1L, 1L));
        Assert.assertEquals(String.format("Storage volume %s is disabled", svName), ex.getMessage());

        ex = Assert.assertThrows(IllegalStateException.class, () -> svm.removeStorageVolume(svName));
        Assert.assertEquals("Storage volume 'test' is referenced by dbs or tables, dbs: [1], tables: [1]", ex.getMessage());
        svm.unbindDbToStorageVolume(1L);
        svm.unbindTableToStorageVolume(1L);
        svm.removeStorageVolume(svName);
        Assert.assertFalse(svm.exists(svName));
    }

    @Test
    public void testParseParamsFromConfig() {
        SharedDataStorageVolumeMgr sdsvm = new SharedDataStorageVolumeMgr();
        Map<String, String> params = Deencapsulation.invoke(sdsvm, "parseParamsFromConfig");
        Assert.assertEquals("access_key", params.get(AWS_S3_ACCESS_KEY));
        Assert.assertEquals("secret_key", params.get(AWS_S3_SECRET_KEY));
        Assert.assertEquals("region", params.get(AWS_S3_REGION));
        Assert.assertEquals("endpoint", params.get(AWS_S3_ENDPOINT));

        Config.cloud_native_storage_type = "aaa";
        params = Deencapsulation.invoke(sdsvm, "parseParamsFromConfig");
        Assert.assertEquals(0, params.size());
    }

    @Test
    public void testParseLocationsFromConfig() throws InvalidConfException {
        Config.cloud_native_storage_type = "s3";
        Config.aws_s3_path = "default-bucket/1";
        {
            List<String> locations = SharedDataStorageVolumeMgr.parseLocationsFromConfig();
            Assert.assertEquals(1, locations.size());
            Assert.assertEquals("s3://default-bucket/1", locations.get(0));
        }

        // with s3:// prefix
        Config.aws_s3_path = "s3://default-bucket/1";
        {
            List<String> locations = SharedDataStorageVolumeMgr.parseLocationsFromConfig();
            Assert.assertEquals(1, locations.size());
            Assert.assertEquals("s3://default-bucket/1", locations.get(0));
        }

        Config.aws_s3_path = "s3://default-bucket";
        {
            List<String> locations = SharedDataStorageVolumeMgr.parseLocationsFromConfig();
            Assert.assertEquals(1, locations.size());
            Assert.assertEquals("s3://default-bucket", locations.get(0));
        }

        Config.aws_s3_path = "s3://default-bucket/";
        {
            List<String> locations = SharedDataStorageVolumeMgr.parseLocationsFromConfig();
            Assert.assertEquals(1, locations.size());
            Assert.assertEquals("s3://default-bucket/", locations.get(0));
        }

        // with invalid prefix
        Config.aws_s3_path = "://default-bucket/1";
        Assert.assertThrows(InvalidConfException.class, SharedDataStorageVolumeMgr::parseLocationsFromConfig);
        // with wrong prefix
        Config.aws_s3_path = "hdfs://default-bucket/1";
        Assert.assertThrows(InvalidConfException.class, SharedDataStorageVolumeMgr::parseLocationsFromConfig);

        Config.aws_s3_path = "s3://";
        Assert.assertThrows(InvalidConfException.class, SharedDataStorageVolumeMgr::parseLocationsFromConfig);

        Config.aws_s3_path = "bucketname:30/b";
        Assert.assertThrows(InvalidConfException.class, SharedDataStorageVolumeMgr::parseLocationsFromConfig);

        Config.aws_s3_path = "s3://bucketname:9030/b";
        Assert.assertThrows(InvalidConfException.class, SharedDataStorageVolumeMgr::parseLocationsFromConfig);

        Config.aws_s3_path = "/";
        Assert.assertThrows(InvalidConfException.class, SharedDataStorageVolumeMgr::parseLocationsFromConfig);

        Config.aws_s3_path = "";
        Assert.assertThrows(InvalidConfException.class, SharedDataStorageVolumeMgr::parseLocationsFromConfig);

        Config.cloud_native_storage_type = "hdfs";
        Config.cloud_native_hdfs_url = "hdfs://url";
        {
            List<String> locations = SharedDataStorageVolumeMgr.parseLocationsFromConfig();
            Assert.assertEquals(1, locations.size());
            Assert.assertEquals("hdfs://url", locations.get(0));
        }
        Config.cloud_native_hdfs_url = "viewfs://host:9030/a/b/c";
        {
            List<String> locations = SharedDataStorageVolumeMgr.parseLocationsFromConfig();
            Assert.assertEquals(1, locations.size());
            Assert.assertEquals("viewfs://host:9030/a/b/c", locations.get(0));
        }
    }

    @Test
    public void testCreateBuiltinStorageVolume() throws DdlException, AlreadyExistsException, MetaNotFoundException {
        new Expectations() {
            {
                editLog.logSetDefaultStorageVolume((SetDefaultStorageVolumeLog) any);
            }
        };

        SharedDataStorageVolumeMgr sdsvm = new SharedDataStorageVolumeMgr();
        Assert.assertFalse(sdsvm.exists(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME));

        Config.enable_load_volume_from_conf = false;
        sdsvm.createBuiltinStorageVolume();
        Assert.assertFalse(sdsvm.exists(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME));

        Config.enable_load_volume_from_conf = true;
        String id = sdsvm.createBuiltinStorageVolume();
        String[] bucketAndPrefix = Deencapsulation.invoke(sdsvm, "getBucketAndPrefix");
        Assert.assertEquals(bucketAndPrefix[0], id);
        Assert.assertTrue(sdsvm.exists(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME));
        StorageVolume sv = sdsvm.getStorageVolumeByName(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME);
        Assert.assertEquals(id, sdsvm.getDefaultStorageVolumeId());

        FileStoreInfo fsInfo = sv.getCloudConfiguration().toFileStoreInfo();
        Assert.assertEquals("region", fsInfo.getS3FsInfo().getRegion());
        Assert.assertEquals("endpoint", fsInfo.getS3FsInfo().getEndpoint());
        Assert.assertTrue(fsInfo.getS3FsInfo().hasCredential());
        Assert.assertTrue(fsInfo.getS3FsInfo().getCredential().hasSimpleCredential());
        Assert.assertFalse(fsInfo.getS3FsInfo().getPartitionedPrefixEnabled());
        Assert.assertEquals(0, fsInfo.getS3FsInfo().getNumPartitionedPrefix());

        // Builtin storage volume has existed, the conf will be ignored
        Config.aws_s3_region = "region1";
        Config.aws_s3_endpoint = "endpoint1";
        sdsvm.createBuiltinStorageVolume();
        sv = sdsvm.getStorageVolumeByName(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME);
        Assert.assertTrue(sv.getCloudConfiguration().toFileStoreInfo().getS3FsInfo().hasCredential());
        Assert.assertEquals("region", sv.getCloudConfiguration().toFileStoreInfo().getS3FsInfo().getRegion());
        Assert.assertEquals("endpoint", sv.getCloudConfiguration().toFileStoreInfo().getS3FsInfo().getEndpoint());
        Assert.assertTrue(sv.getCloudConfiguration().toFileStoreInfo().getS3FsInfo().hasCredential());
        Assert.assertTrue(sv.getCloudConfiguration().toFileStoreInfo().getS3FsInfo().getCredential().hasSimpleCredential());

        String svName = "test";
        List<String> locations = Arrays.asList("s3://abc");
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put(AWS_S3_REGION, "region");
        storageParams.put(AWS_S3_ENDPOINT, "endpoint");
        storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
        sdsvm.createStorageVolume(svName, "S3", locations, storageParams, Optional.empty(), "");
        sdsvm.setDefaultStorageVolume(svName);

        Config.aws_s3_use_instance_profile = true;
        Config.aws_s3_use_aws_sdk_default_behavior = false;
        sdsvm.removeStorageVolume(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME);
        sdsvm.createBuiltinStorageVolume();
        sv = sdsvm.getStorageVolumeByName(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME);
        Assert.assertTrue(sv.getCloudConfiguration().toFileStoreInfo().getS3FsInfo().hasCredential());
        Assert.assertTrue(sv.getCloudConfiguration().toFileStoreInfo().getS3FsInfo().getCredential().hasProfileCredential());

        Config.aws_s3_iam_role_arn = "role_arn";
        Config.aws_s3_external_id = "external_id";
        sdsvm.removeStorageVolume(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME);
        sdsvm.createBuiltinStorageVolume();
        sv = sdsvm.getStorageVolumeByName(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME);
        Assert.assertTrue(sv.getCloudConfiguration().toFileStoreInfo().getS3FsInfo().hasCredential());
        Assert.assertTrue(sv.getCloudConfiguration().toFileStoreInfo().getS3FsInfo().getCredential().hasAssumeRoleCredential());

        Config.cloud_native_storage_type = "hdfs";
        Config.cloud_native_hdfs_url = "hdfs://url";
        sdsvm.removeStorageVolume(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME);
        id = sdsvm.createBuiltinStorageVolume();
        Assert.assertEquals(Config.cloud_native_hdfs_url, id);
        sv = sdsvm.getStorageVolumeByName(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME);
        Assert.assertTrue(sv.getCloudConfiguration().toFileStoreInfo().hasHdfsFsInfo());

        Config.cloud_native_storage_type = "azblob";
        Config.azure_blob_shared_key = "shared_key";
        Config.azure_blob_sas_token = "sas_token";
        Config.azure_blob_endpoint = "endpoint";
        Config.azure_blob_path = "path";
        sdsvm.removeStorageVolume(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME);
        sdsvm.createBuiltinStorageVolume();
        sv = sdsvm.getStorageVolumeByName(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME);
        Assert.assertEquals("endpoint", sv.getCloudConfiguration().toFileStoreInfo().getAzblobFsInfo().getEndpoint());
        Assert.assertEquals("shared_key",
                sv.getCloudConfiguration().toFileStoreInfo().getAzblobFsInfo().getCredential().getSharedKey());
        Assert.assertEquals("sas_token",
                sv.getCloudConfiguration().toFileStoreInfo().getAzblobFsInfo().getCredential().getSasToken());

        Config.cloud_native_storage_type = "adls2";
        Config.azure_adls2_shared_key = "shared_key";
        Config.azure_adls2_sas_token = "sas_token";
        Config.azure_adls2_endpoint = "endpoint";
        Config.azure_adls2_path = "path";
        sdsvm.removeStorageVolume(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME);
        sdsvm.createBuiltinStorageVolume();
        sv = sdsvm.getStorageVolumeByName(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME);
        Assert.assertEquals("endpoint", sv.getCloudConfiguration().toFileStoreInfo().getAdls2FsInfo().getEndpoint());
        Assert.assertEquals("shared_key",
                sv.getCloudConfiguration().toFileStoreInfo().getAdls2FsInfo().getCredential().getSharedKey());
        Assert.assertEquals("sas_token",
                sv.getCloudConfiguration().toFileStoreInfo().getAdls2FsInfo().getCredential().getSasToken());

        Config.cloud_native_storage_type = "GS";
        Config.gcp_gcs_use_compute_engine_service_account = "true";
        Config.gcp_gcs_path = "gs://gs_path";
        sdsvm.removeStorageVolume(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME);
        sdsvm.createBuiltinStorageVolume();
        sv = sdsvm.getStorageVolumeByName(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME);
        Assert.assertEquals(true, sv.getCloudConfiguration().toFileStoreInfo().getGsFsInfo().getUseComputeEngineServiceAccount());
    }

    @Test
    public void testGetDefaultStorageVolume() throws IllegalAccessException, AlreadyExistsException,
            DdlException, NoSuchFieldException {
        new Expectations() {
            {
                editLog.logSetDefaultStorageVolume((SetDefaultStorageVolumeLog) any);
            }
        };

        SharedDataStorageVolumeMgr sdsvm = new SharedDataStorageVolumeMgr();
        sdsvm.createBuiltinStorageVolume();
        FieldUtils.writeField(sdsvm, "defaultStorageVolumeId", "", true);
        Assert.assertEquals(SharedDataStorageVolumeMgr.BUILTIN_STORAGE_VOLUME, sdsvm.getDefaultStorageVolume().getName());

        String svName = "test";
        List<String> locations = Arrays.asList("s3://abc");
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put(AWS_S3_REGION, "region");
        storageParams.put(AWS_S3_ENDPOINT, "endpoint");
        storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
        sdsvm.createStorageVolume(svName, "S3", locations, storageParams, Optional.empty(), "");
        sdsvm.setDefaultStorageVolume(svName);
        Assert.assertEquals(svName, sdsvm.getDefaultStorageVolume().getName());
    }

    @Test
    public void testGetStorageVolumeOfDb() throws DdlException, AlreadyExistsException {
        new Expectations() {
            {
                editLog.logSetDefaultStorageVolume((SetDefaultStorageVolumeLog) any);
            }
        };

        SharedDataStorageVolumeMgr sdsvm = new SharedDataStorageVolumeMgr();
        ErrorReportException ex = Assert.assertThrows(ErrorReportException.class, () -> Deencapsulation.invoke(sdsvm,
                "getStorageVolumeOfDb", StorageVolumeMgr.DEFAULT));
        Assert.assertEquals(ErrorCode.ERR_NO_DEFAULT_STORAGE_VOLUME, ex.getErrorCode());
        sdsvm.createBuiltinStorageVolume();
        String defaultSVId = sdsvm.getStorageVolumeByName(SharedDataStorageVolumeMgr.BUILTIN_STORAGE_VOLUME).getId();

        String svName = "test";
        List<String> locations = Arrays.asList("s3://abc");
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put(AWS_S3_REGION, "region");
        storageParams.put(AWS_S3_ENDPOINT, "endpoint");
        storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
        String testSVId = sdsvm.createStorageVolume(svName, "S3", locations, storageParams, Optional.empty(), "");

        StorageVolume sv = Deencapsulation.invoke(sdsvm, "getStorageVolumeOfDb", StorageVolumeMgr.DEFAULT);
        Assert.assertEquals(defaultSVId, sv.getId());
        sv = Deencapsulation.invoke(sdsvm, "getStorageVolumeOfDb", svName);
        Assert.assertEquals(testSVId, sv.getId());
    }

    @Test
    public void testGetStorageVolumeOfTable()
            throws DdlException, AlreadyExistsException {
        new Expectations() {
            {
                editLog.logSetDefaultStorageVolume((SetDefaultStorageVolumeLog) any);
            }
        };

        SharedDataStorageVolumeMgr sdsvm = new SharedDataStorageVolumeMgr();

        String svName = "test";
        List<String> locations = Arrays.asList("s3://abc");
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put(AWS_S3_REGION, "region");
        storageParams.put(AWS_S3_ENDPOINT, "endpoint");
        storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
        String testSVId = sdsvm.createStorageVolume(svName, "S3", locations, storageParams, Optional.empty(), "");

        new MockUp<SharedDataStorageVolumeMgr>() {
            @Mock
            public String getStorageVolumeIdOfDb(long dbId) {
                if (dbId == 1L) {
                    return testSVId;
                }
                return null;
            }
        };

        StorageVolume sv = Deencapsulation.invoke(sdsvm, "getStorageVolumeOfTable", "", 1L);
        Assert.assertEquals(testSVId, sv.getId());
        Config.enable_load_volume_from_conf = false;
        ErrorReportException ex = Assert.assertThrows(ErrorReportException.class, () -> Deencapsulation.invoke(sdsvm,
                "getStorageVolumeOfTable", "", 2L));
        Assert.assertEquals(ErrorCode.ERR_NO_DEFAULT_STORAGE_VOLUME, ex.getErrorCode());
        Config.enable_load_volume_from_conf = true;
        ex = Assert.assertThrows(ErrorReportException.class, () -> Deencapsulation.invoke(sdsvm,
                "getStorageVolumeOfTable", "", 2L));
        Assert.assertEquals(ErrorCode.ERR_NO_DEFAULT_STORAGE_VOLUME, ex.getErrorCode());
        sdsvm.createBuiltinStorageVolume();
        String defaultSVId = sdsvm.getStorageVolumeByName(SharedDataStorageVolumeMgr.BUILTIN_STORAGE_VOLUME).getId();
        sv = Deencapsulation.invoke(sdsvm, "getStorageVolumeOfTable", StorageVolumeMgr.DEFAULT, 1L);
        Assert.assertEquals(defaultSVId, sv.getId());
        sv = Deencapsulation.invoke(sdsvm, "getStorageVolumeOfTable", svName, 1L);
        Assert.assertEquals(testSVId, sv.getId());
    }

    @Test
    public void testReplayBindDbToStorageVolume() throws DdlException, AlreadyExistsException {
        SharedDataStorageVolumeMgr sdsvm = new SharedDataStorageVolumeMgr();
        sdsvm.replayBindDbToStorageVolume(null, 2L);
        Assert.assertTrue(sdsvm.dbToStorageVolume.isEmpty());
        Assert.assertTrue(sdsvm.storageVolumeToDbs.isEmpty());

        String svName = "test";
        List<String> locations = Arrays.asList("s3://abc");
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put(AWS_S3_REGION, "region");
        storageParams.put(AWS_S3_ENDPOINT, "endpoint");
        storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
        String svId = sdsvm.createStorageVolume(svName, "S3", locations, storageParams, Optional.empty(), "");

        sdsvm.replayBindDbToStorageVolume(svId, 1L);
        Assert.assertEquals(svId, sdsvm.getStorageVolumeIdOfDb(1L));
    }

    @Test
    public void testReplayBindTableToStorageVolume() throws DdlException, AlreadyExistsException {
        SharedDataStorageVolumeMgr sdsvm = new SharedDataStorageVolumeMgr();
        sdsvm.replayBindTableToStorageVolume(null, 2L);
        Assert.assertTrue(sdsvm.tableToStorageVolume.isEmpty());
        Assert.assertTrue(sdsvm.storageVolumeToTables.isEmpty());

        String svName = "test";
        List<String> locations = Arrays.asList("s3://abc");
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put(AWS_S3_REGION, "region");
        storageParams.put(AWS_S3_ENDPOINT, "endpoint");
        storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
        String svId = sdsvm.createStorageVolume(svName, "S3", locations, storageParams, Optional.empty(), "");

        sdsvm.replayBindTableToStorageVolume(svId, 1L);
        Assert.assertEquals(svId, sdsvm.getStorageVolumeIdOfTable(1L));
    }

    @Test
    public void testGetBucketAndPrefix() throws Exception {
        String oldAwsS3Path = Config.aws_s3_path;

        SharedDataStorageVolumeMgr sdsvm = new SharedDataStorageVolumeMgr();
        Config.aws_s3_path = "bucket/dir1/dir2";
        String[] bucketAndPrefix1 = Deencapsulation.invoke(sdsvm, "getBucketAndPrefix");
        Assert.assertEquals(2, bucketAndPrefix1.length);
        Assert.assertEquals("bucket", bucketAndPrefix1[0]);
        Assert.assertEquals("dir1/dir2", bucketAndPrefix1[1]);

        Config.aws_s3_path = "bucket";
        String[] bucketAndPrefix2 = Deencapsulation.invoke(sdsvm, "getBucketAndPrefix");
        Assert.assertEquals(2, bucketAndPrefix2.length);
        Assert.assertEquals("bucket", bucketAndPrefix2[0]);
        Assert.assertEquals("", bucketAndPrefix2[1]);

        Config.aws_s3_path = "bucket/";
        String[] bucketAndPrefix3 = Deencapsulation.invoke(sdsvm, "getBucketAndPrefix");
        Assert.assertEquals(2, bucketAndPrefix3.length);
        Assert.assertEquals("bucket", bucketAndPrefix3[0]);
        Assert.assertEquals("", bucketAndPrefix3[1]);

        // allow leading s3:// in configuration, will be just ignored.
        Config.aws_s3_path = "s3://a-bucket/b";
        {
            String[] bucketAndPrefix = Deencapsulation.invoke(sdsvm, "getBucketAndPrefix");
            Assert.assertEquals(2, bucketAndPrefix.length);
            Assert.assertEquals("a-bucket", bucketAndPrefix[0]);
            Assert.assertEquals("b", bucketAndPrefix[1]);
        }

        Config.aws_s3_path = oldAwsS3Path;
    }

    @Test
    public void testGetAwsCredentialType() throws Exception {
        boolean oldAwsS3UseAwsSdkDefaultBehavior = Config.aws_s3_use_aws_sdk_default_behavior;
        boolean oldAwsS3UseInstanceProfile = Config.aws_s3_use_instance_profile;
        String oldAwsS3AccessKey = Config.aws_s3_access_key;
        String oldAwsS3SecretKey = Config.aws_s3_secret_key;
        String oldAwsS3IamRoleArn = Config.aws_s3_iam_role_arn;

        SharedDataStorageVolumeMgr sdsvm = new SharedDataStorageVolumeMgr();
        Config.aws_s3_use_aws_sdk_default_behavior = true;
        String credentialType1 = Deencapsulation.invoke(sdsvm, "getAwsCredentialType");
        Assert.assertEquals("default", credentialType1);

        Config.aws_s3_use_aws_sdk_default_behavior = false;
        Config.aws_s3_use_instance_profile = true;
        Config.aws_s3_iam_role_arn = "";
        String credentialType2 = Deencapsulation.invoke(sdsvm, "getAwsCredentialType");
        Assert.assertEquals("instance_profile", credentialType2);

        Config.aws_s3_use_aws_sdk_default_behavior = false;
        Config.aws_s3_use_instance_profile = true;
        Config.aws_s3_iam_role_arn = "abc";
        String credentialType3 = Deencapsulation.invoke(sdsvm, "getAwsCredentialType");
        Assert.assertEquals("assume_role", credentialType3);

        Config.aws_s3_use_aws_sdk_default_behavior = false;
        Config.aws_s3_use_instance_profile = false;
        Config.aws_s3_access_key = "";
        String credentialType4 = Deencapsulation.invoke(sdsvm, "getAwsCredentialType");
        Assert.assertNull(credentialType4);

        Config.aws_s3_use_aws_sdk_default_behavior = false;
        Config.aws_s3_use_instance_profile = false;
        Config.aws_s3_access_key = "abc";
        Config.aws_s3_secret_key = "abc";
        Config.aws_s3_iam_role_arn = "";
        String credentialType5 = Deencapsulation.invoke(sdsvm, "getAwsCredentialType");
        Assert.assertEquals("simple", credentialType5);

        Config.aws_s3_use_aws_sdk_default_behavior = false;
        Config.aws_s3_use_instance_profile = false;
        Config.aws_s3_access_key = "abc";
        Config.aws_s3_secret_key = "abc";
        Config.aws_s3_iam_role_arn = "abc";
        String credentialType6 = Deencapsulation.invoke(sdsvm, "getAwsCredentialType");
        Assert.assertNull(credentialType6);

        Config.aws_s3_use_aws_sdk_default_behavior = oldAwsS3UseAwsSdkDefaultBehavior;
        Config.aws_s3_use_instance_profile = oldAwsS3UseInstanceProfile;
        Config.aws_s3_access_key = oldAwsS3AccessKey;
        Config.aws_s3_secret_key = oldAwsS3SecretKey;
        Config.aws_s3_iam_role_arn = oldAwsS3IamRoleArn;
    }

    @Test
    public void testValidateStorageVolumeConfig() {
        SharedDataStorageVolumeMgr sdsvm = new SharedDataStorageVolumeMgr();

        Config.cloud_native_storage_type = "s3";
        Config.aws_s3_path = "";
        Assert.assertThrows(InvalidConfException.class, sdsvm::validateStorageVolumeConfig);

        Config.aws_s3_path = "path";
        Config.aws_s3_region = "";
        Config.aws_s3_endpoint = "";
        Assert.assertThrows(InvalidConfException.class, sdsvm::validateStorageVolumeConfig);

        Config.cloud_native_storage_type = "hdfs";
        Config.cloud_native_hdfs_url = "";
        Assert.assertThrows(InvalidConfException.class, sdsvm::validateStorageVolumeConfig);

        Config.cloud_native_storage_type = "azblob";
        Config.azure_blob_path = "";
        Assert.assertThrows(InvalidConfException.class, sdsvm::validateStorageVolumeConfig);

        Config.azure_blob_path = "blob";
        Config.azure_blob_endpoint = "";
        Assert.assertThrows(InvalidConfException.class, sdsvm::validateStorageVolumeConfig);

        Config.cloud_native_storage_type = "adls2";
        Config.azure_adls2_path = "";
        Assert.assertThrows(InvalidConfException.class, sdsvm::validateStorageVolumeConfig);

        Config.azure_adls2_path = "adls2";
        Config.azure_adls2_endpoint = "";
        Assert.assertThrows(InvalidConfException.class, sdsvm::validateStorageVolumeConfig);
    }

    @Test
    public void testSerialization() throws IOException, SRMetaBlockException,
            SRMetaBlockEOFException, DdlException, AlreadyExistsException {
        String svName = "test";
        // create
        StorageVolumeMgr svm = new SharedDataStorageVolumeMgr();
        List<String> locations = Arrays.asList("s3://abc");
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put(AWS_S3_REGION, "region");
        storageParams.put(AWS_S3_ENDPOINT, "endpoint");
        storageParams.put(AWS_S3_USE_AWS_SDK_DEFAULT_BEHAVIOR, "true");
        String svId = svm.createStorageVolume(svName, "S3", locations, storageParams, Optional.empty(), "");
        svm.setDefaultStorageVolume("test");
        svm.bindDbToStorageVolume(svName, 1L);
        svm.bindTableToStorageVolume(svName, 1L, 1L);
        Map<String, Set<Long>> storageVolumeToDbs = svm.storageVolumeToDbs;
        Map<String, Set<Long>> storageVolumeToTables = svm.storageVolumeToTables;
        Map<Long, String> dbToStorageVolume = svm.dbToStorageVolume;
        Map<Long, String> tableToStorageVolume = svm.tableToStorageVolume;

        // v4
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(out);
        ImageWriter imageWriter = new ImageWriter("", 0);
        imageWriter.setOutputStream(dos);
        svm.save(imageWriter);

        InputStream in = new ByteArrayInputStream(out.toByteArray());
        DataInputStream dis = new DataInputStream(in);
        SRMetaBlockReader reader = new SRMetaBlockReaderV2(new JsonReader(new InputStreamReader(in)));
        StorageVolumeMgr svm1 = new SharedDataStorageVolumeMgr();
        svm1.load(reader);
        Assert.assertEquals(svId, svm1.getDefaultStorageVolumeId());
        Assert.assertEquals(storageVolumeToDbs, svm1.storageVolumeToDbs);
        Assert.assertEquals(storageVolumeToTables, svm1.storageVolumeToTables);
        Assert.assertEquals(dbToStorageVolume, svm1.dbToStorageVolume);
        Assert.assertEquals(tableToStorageVolume, svm1.tableToStorageVolume);
    }

    @Test
    public void testGetTableBindingsOfBuiltinStorageVolume() throws DdlException, AlreadyExistsException {
        new MockUp<LocalMetastore>() {
            @Mock
            public List<Long> getDbIdsIncludeRecycleBin() {
                return Arrays.asList(10001L);
            }

            @Mock
            public Database getDbIncludeRecycleBin(long dbId) {
                return new Database(dbId, "");
            }

            @Mock
            public List<Table> getTablesIncludeRecycleBin(Database db) {
                long dbId = 10001L;
                long tableId = 10002L;
                long partitionId = 10003L;
                long indexId = 10004L;
                long tablet1Id = 10010L;
                long tablet2Id = 10011L;

                // Schema
                List<Column> columns = Lists.newArrayList();
                Column k1 = new Column("k1", Type.INT, true, null, "", "");
                columns.add(k1);
                columns.add(new Column("k2", Type.BIGINT, true, null, "", ""));
                columns.add(new Column("v", Type.BIGINT, false, AggregateType.SUM, "0", ""));

                // Tablet
                Tablet tablet1 = new LakeTablet(tablet1Id);
                Tablet tablet2 = new LakeTablet(tablet2Id);

                // Index
                MaterializedIndex index = new MaterializedIndex(indexId, MaterializedIndex.IndexState.NORMAL);
                TabletMeta tabletMeta = new TabletMeta(dbId, tableId, partitionId, indexId, 0, TStorageMedium.HDD, true);
                index.addTablet(tablet1, tabletMeta);
                index.addTablet(tablet2, tabletMeta);

                // Partition
                DistributionInfo distributionInfo = new HashDistributionInfo(10, Lists.newArrayList(k1));
                PartitionInfo partitionInfo = new SinglePartitionInfo();
                partitionInfo.setReplicationNum(partitionId, (short) 3);

                // Lake table
                LakeTable table = new LakeTable(tableId, "t1", columns, KeysType.AGG_KEYS, partitionInfo, distributionInfo);
                return Arrays.asList(table);
            }
        };

        SharedDataStorageVolumeMgr sdsvm = new SharedDataStorageVolumeMgr();
        Assert.assertEquals(Arrays.asList(Arrays.asList(10001L), Arrays.asList(10002L)),
                sdsvm.getBindingsOfBuiltinStorageVolume());

        sdsvm.createBuiltinStorageVolume();
        sdsvm.bindDbToStorageVolume(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME, 10001L);
        Assert.assertEquals(Arrays.asList(new ArrayList(), new ArrayList()), sdsvm.getBindingsOfBuiltinStorageVolume());

        sdsvm.unbindDbToStorageVolume(10001L);
        sdsvm.bindTableToStorageVolume(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME, 10001L, 10002L);
        Assert.assertEquals(Arrays.asList(Arrays.asList(10001L), new ArrayList()), sdsvm.getBindingsOfBuiltinStorageVolume());
    }

    @Test
    public void testGetStorageVolumeNameOfTable() throws DdlException, AlreadyExistsException {
        StorageVolumeMgr svm = new SharedDataStorageVolumeMgr();
        svm.createBuiltinStorageVolume();
        svm.bindTableToStorageVolume(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME, 1L, 1L);
        Assert.assertEquals(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME, svm.getStorageVolumeNameOfTable(1L));
        Assert.assertEquals(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME, svm.getStorageVolumeNameOfTable(2L));
    }

    @Test
    public void testGetStorageVolumeNameOfDb() throws DdlException, AlreadyExistsException {
        StorageVolumeMgr svm = new SharedDataStorageVolumeMgr();
        svm.createBuiltinStorageVolume();
        svm.bindDbToStorageVolume(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME, 1L);
        Assert.assertEquals(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME, svm.getStorageVolumeNameOfDb(1L));
        Assert.assertEquals(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME, svm.getStorageVolumeNameOfDb(2L));
    }

    @Test
    public void testCreateHDFS() throws DdlException, AlreadyExistsException {
        String svName = "test";
        // create
        StorageVolumeMgr svm = new SharedDataStorageVolumeMgr();
        List<String> locations = Arrays.asList("hdfs://abc");
        Map<String, String> storageParams = new HashMap<>();
        storageParams.put("dfs.nameservices", "ha_cluster");
        storageParams.put("dfs.ha.namenodes.ha_cluster", "ha_n1,ha_n2");
        storageParams.put("dfs.namenode.rpc-address.ha_cluster.ha_n1", "<hdfs_host>:<hdfs_port>");
        storageParams.put("dfs.namenode.rpc-address.ha_cluster.ha_n2", "<hdfs_host>:<hdfs_port>");
        String svKey = svm.createStorageVolume(svName, "hdfs", locations, storageParams, Optional.empty(), "");
        Assert.assertEquals(true, svm.exists(svName));

        storageParams.put("dfs.client.failover.proxy.provider",
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
        svm.updateStorageVolume("test", null, null, storageParams, Optional.of(false), "");
        Assert.assertEquals(false, svm.getStorageVolumeByName(svName).getEnabled());
    }

    @Test
    public void testCreateStorageVolumeWithInvalidParams() throws DdlException {
        String svName = "test";
        StorageVolumeMgr svm = new SharedDataStorageVolumeMgr();
        List<String> locations = new ArrayList<>(Arrays.asList("{hdfs://abc}"));
        Map<String, String> storageParams = new HashMap<>();
        Assert.assertThrows(DdlException.class,
                () -> svm.createStorageVolume(svName, "hdfs", locations, storageParams, Optional.empty(), ""));

        locations.clear();
        locations.add("ablob://abc");
        Assert.assertThrows(DdlException.class,
                () -> svm.createStorageVolume(svName, "s3", locations, storageParams, Optional.empty(), ""));

        locations.clear();
        locations.add("s3://abc");
        Assert.assertThrows(DdlException.class,
                () -> svm.createStorageVolume(svName, "azblob", locations, storageParams, Optional.empty(), ""));

        Assert.assertThrows(DdlException.class,
                () -> svm.createStorageVolume(svName, "abc", locations, storageParams, Optional.empty(), ""));

        {
            // only for s3
            Map<String, String> params = new HashMap<>();
            params.put(CloudConfigurationConstants.AWS_S3_NUM_PARTITIONED_PREFIX, "32");
            Assert.assertThrows(DdlException.class,
                    () -> svm.createStorageVolume(svName, "azblob", locations, params, Optional.empty(), ""));
        }
        {
            // only for s3
            Map<String, String> params = new HashMap<>();
            params.put(CloudConfigurationConstants.AWS_S3_ENABLE_PARTITIONED_PREFIX, "true");
            Assert.assertThrows(DdlException.class,
                    () -> svm.createStorageVolume(svName, "azblob", locations, params, Optional.empty(), ""));
        }
        {
            // should be a number
            Map<String, String> params = new HashMap<>();
            params.put(CloudConfigurationConstants.AWS_S3_NUM_PARTITIONED_PREFIX, "not_a_number");
            Assert.assertThrows(DdlException.class,
                    () -> svm.createStorageVolume(svName, "s3", locations, params, Optional.empty(), ""));
        }
        {
            // should be a positive integer
            Map<String, String> params = new HashMap<>();
            params.put(CloudConfigurationConstants.AWS_S3_NUM_PARTITIONED_PREFIX, "-1");
            Assert.assertThrows(DdlException.class,
                    () -> svm.createStorageVolume(svName, "s3", locations, params, Optional.empty(), ""));
        }
    }

    @Test
    public void testUpgrade() throws IOException, SRMetaBlockException, SRMetaBlockEOFException,
            DdlException, AlreadyExistsException {
        StorageVolumeMgr svm = new SharedDataStorageVolumeMgr();
        svm.createBuiltinStorageVolume();
        Set<Long> dbs = new HashSet<>();
        dbs.add(1L);
        svm.storageVolumeToDbs.put(null, dbs);
        Set<Long> tables = new HashSet<>();
        tables.add(2L);
        svm.storageVolumeToTables.put(null, tables);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(out);
        ImageWriter imageWriter = new ImageWriter("", 0);
        imageWriter.setOutputStream(dos);
        svm.save(imageWriter);

        InputStream in = new ByteArrayInputStream(out.toByteArray());
        SRMetaBlockReader reader = new SRMetaBlockReaderV2(new JsonReader(new InputStreamReader(in)));
        StorageVolumeMgr svm1 = new SharedDataStorageVolumeMgr();
        svm1.load(reader);
        Assert.assertEquals(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME, svm1.getDefaultStorageVolume().getName());
        Assert.assertTrue(svm1.storageVolumeToDbs.isEmpty());
        Assert.assertTrue(svm1.storageVolumeToTables.isEmpty());
        Assert.assertTrue(svm1.dbToStorageVolume.isEmpty());
        Assert.assertTrue(svm1.tableToStorageVolume.isEmpty());
        Assert.assertEquals(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME, svm1.getStorageVolumeNameOfDb(1L));
        Assert.assertEquals(StorageVolumeMgr.BUILTIN_STORAGE_VOLUME, svm1.getStorageVolumeNameOfTable(1L));
    }
}
