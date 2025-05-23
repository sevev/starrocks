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

namespace cpp starrocks
namespace java com.starrocks.thrift

include "Status.thrift"
include "Types.thrift"

const i64 IS_SET_DEFAULT_ROWSET_TO_BETA_BIT = 0x01;

struct TMasterInfo {
    1: required Types.TNetworkAddress network_address
    2: optional Types.TClusterId cluster_id     //deprecated
    3: required Types.TEpoch epoch
    4: optional string token 
    5: optional string backend_ip
    6: optional Types.TPort http_port
    7: optional i64 heartbeat_flags
    8: optional i64 backend_id
    9: optional i64 min_active_txn_id = 0
    10: optional Types.TRunMode run_mode
    11: optional list<string> disabled_disks
    12: optional list<string> decommissioned_disks
    13: optional bool encrypted;
    14: optional bool stop_regular_tablet_report; // used for upgrade/downgrade compatibility, can be removed after 3.5
    15: optional Types.TNodeType node_type
}

struct TBackendInfo {
    1: required Types.TPort be_port
    2: required Types.TPort http_port
    3: optional Types.TPort be_rpc_port
    4: optional Types.TPort brpc_port
    5: optional string version
    6: optional i32 num_hardware_cores
    7: optional Types.TPort starlet_port
    8: optional i64 reboot_time
    9: optional bool is_set_storage_path

    10: optional i64 mem_limit_bytes

    11: optional Types.TPort arrow_flight_port
}

struct THeartbeatResult {
    1: required Status.TStatus status 
    2: required TBackendInfo backend_info
}

service HeartbeatService {
    THeartbeatResult heartbeat(1:TMasterInfo master_info);
}
