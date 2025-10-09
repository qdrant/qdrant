import json
import os
import re
import shutil
import signal
from subprocess import Popen
import time
from typing import Tuple, Callable, Dict, List
import requests
import socket
from contextlib import closing
from pathlib import Path
import pytest
from .assertions import assert_http_ok


WAIT_TIME_SEC = 30
RETRY_INTERVAL_SEC = 0.2


# Tracks processes that need to be killed at the end of the test
processes: List['PeerProcess'] = []
busy_ports = {}


class PeerProcess:
    def __init__(self, proc: Popen, http_port, grpc_port, p2p_port):
        self.proc = proc
        self.http_port = http_port
        self.grpc_port = grpc_port
        self.p2p_port = p2p_port
        self.pid = proc.pid

    def kill(self):
        self.proc.kill()
        # remove allocated ports from the dictionary
        # so they can be used afterwards
        del busy_ports[self.http_port]
        del busy_ports[self.grpc_port]
        del busy_ports[self.p2p_port]

    def interrupt(self):
        self.proc.send_signal(signal.SIGINT)
        self.proc.wait()

        del busy_ports[self.http_port]
        del busy_ports[self.grpc_port]
        del busy_ports[self.p2p_port]


def _occupy_port(port):
    if port in busy_ports:
        raise Exception(f'Port "{port}" was already allocated!')
    busy_ports[port] = True
    return port


def kill_all_processes():
    print()
    while len(processes) > 0:
        p = processes.pop(0)
        if is_coverage_mode():
            print(f"Interrupting {p.pid}")
            p.interrupt()
        else:
            print(f"Killing {p.pid}")
            p.kill()


@pytest.fixture(autouse=True)
def every_test():
    yield
    kill_all_processes()


def get_port() -> int:
    while True:
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
            # get random port assigned by the OS
            s.bind(('', 0))
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            allocated_port = s.getsockname()[1]
            if allocated_port in busy_ports:
                continue
            return allocated_port

def is_coverage_mode() -> bool:
    return os.getenv("COVERAGE") == "1"


def get_env(p2p_port: int, grpc_port: int, http_port: int) -> Dict[str, str]:
    env = os.environ.copy()
    env["QDRANT__CLUSTER__ENABLED"] = "true"
    env["QDRANT__CLUSTER__P2P__PORT"] = str(p2p_port)
    env["QDRANT__SERVICE__HTTP_PORT"] = str(http_port)
    env["QDRANT__SERVICE__GRPC_PORT"] = str(grpc_port)
    env["QDRANT__LOG_LEVEL"] = "TRACE,raft::raft=info,actix_http=info,tonic=info,want=info,mio=info"
    env["QDRANT__SERVICE__HARDWARE_REPORTING"] = "true"

    if is_coverage_mode():
        env["LLVM_PROFILE_FILE"] = get_llvm_profile_file()

    return env


def get_uri(port: int) -> str:
    return f"http://127.0.0.1:{port}"


def assert_project_root():
    directory_path = os.getcwd()
    folder_name = os.path.basename(directory_path)
    assert folder_name == "qdrant"


def get_qdrant_exec() -> str:
    directory_path = os.getcwd()
    if is_coverage_mode():
        qdrant_exec = directory_path + "/target/llvm-cov-target/debug/qdrant"
    else:
        qdrant_exec = directory_path + "/target/debug/qdrant"
    return qdrant_exec

def get_llvm_profile_file() -> str:
    project_root = os.getcwd()
    # %m: keep merging results from each test into the same file
    # If you have multiple tests running in parallel, you can use -%p OR -%{thread_count}m to have different files
    # Not using -%p since each test will generate a new file
    llvm_profile_file = project_root + "/target/llvm-cov-target/qdrant-consensus-tests-%m.profraw"
    return llvm_profile_file


def get_pytest_current_test_name() -> str:
    # https://docs.pytest.org/en/latest/example/simple.html#pytest-current-test-environment-variable
    return os.environ.get('PYTEST_CURRENT_TEST').split(':')[-1].split(' ')[0]


def init_pytest_log_folder() -> str:
    test_name = get_pytest_current_test_name()
    log_folder = f"consensus_test_logs/{test_name}"
    if not os.path.exists(log_folder):
        os.makedirs(log_folder)
    return log_folder


# Starts a peer and returns its api_uri
def start_peer(peer_dir: Path, log_file: str, bootstrap_uri: str, port=None, extra_env=None, reinit=False, uris_in_env=False) -> str:
    if extra_env is None:
        extra_env = {}
    p2p_port = get_port() if port is None else port + 0
    _occupy_port(p2p_port)
    grpc_port = get_port() if port is None else port + 1
    _occupy_port(grpc_port)
    http_port = get_port() if port is None else port + 2
    _occupy_port(http_port)

    test_log_folder = init_pytest_log_folder()
    log_file = open(f"{test_log_folder}/{log_file}", "w")
    this_peer_consensus_uri = get_uri(p2p_port)
    print(f"Starting follower peer with bootstrap uri {bootstrap_uri},"
          f" http: http://localhost:{http_port}/cluster, p2p: {p2p_port}")

    args = [get_qdrant_exec()]
    env = {
        **get_env(p2p_port, grpc_port, http_port),
        **extra_env
    }

    if uris_in_env:
        env["QDRANT_BOOTSTRAP"] = bootstrap_uri
        env["QDRANT_URI"] = this_peer_consensus_uri
    else:
        args.extend(["--bootstrap", bootstrap_uri, "--uri", this_peer_consensus_uri])

    if reinit:
        args.append("--reinit")

    # Wrap with systemd-run to throttle CPU to investigate issues
    # wrapped_cmd = ["systemd-run", "--user", "--scope", "-p", "CPUQuota=20%", "--"] + args
    # proc = Popen(wrapped_cmd, env=env, cwd=peer_dir, stdout=log_file)
    proc = Popen(args, env=env, cwd=peer_dir, stdout=log_file)
    processes.append(PeerProcess(proc, http_port, grpc_port, p2p_port))
    return get_uri(http_port)


# Starts a peer and returns its api_uri and p2p_uri
def start_first_peer(peer_dir: Path, log_file: str, port=None, extra_env=None, reinit=False, uris_in_env=False) -> Tuple[str, str]:
    if extra_env is None:
        extra_env = {}

    p2p_port = get_port() if port is None else port + 0
    _occupy_port(p2p_port)
    grpc_port = get_port() if port is None else port + 1
    _occupy_port(grpc_port)
    http_port = get_port() if port is None else port + 2
    _occupy_port(http_port)

    test_log_folder = init_pytest_log_folder()
    log_file = open(f"{test_log_folder}/{log_file}", "w")
    bootstrap_uri = get_uri(p2p_port)
    print(f"\nStarting first peer with uri {bootstrap_uri},"
          f" http: http://localhost:{http_port}/cluster, p2p: {p2p_port}")

    args = [get_qdrant_exec()]
    env = {
        **get_env(p2p_port, grpc_port, http_port),
        **extra_env
    }

    if uris_in_env:
        env["QDRANT_URI"] = bootstrap_uri
    else:
        args.extend(["--uri", bootstrap_uri])

    if reinit:
        args.append("--reinit")

    # Wrap with systemd-run to throttle CPU to investigate issues
    # wrapped_cmd = ["systemd-run", "--user", "--scope", "-p", "CPUQuota=20%", "--"] + args
    # proc = Popen(wrapped_cmd, env=env, cwd=peer_dir, stdout=log_file)
    proc = Popen(args, env=env, cwd=peer_dir, stdout=log_file)
    processes.append(PeerProcess(proc, http_port, grpc_port, p2p_port))
    return get_uri(http_port), bootstrap_uri


def start_cluster(tmp_path, num_peers, port_seed=None, extra_env=None, headers={}, uris_in_env=False, log_file_prefix=""):
    assert_project_root()
    peer_dirs = make_peer_folders(tmp_path, num_peers)

    # Gathers REST API uris
    peer_api_uris = []

    # Start bootstrap
    (bootstrap_api_uri, bootstrap_uri) = start_first_peer(peer_dirs[0], f"{log_file_prefix}peer_0_0.log", port=port_seed,
                                                          extra_env=extra_env, uris_in_env=uris_in_env)
    peer_api_uris.append(bootstrap_api_uri)

    # Wait for leader
    leader = wait_peer_added(bootstrap_api_uri, headers=headers)

    port = None
    # Start other peers
    for i in range(1, len(peer_dirs)):
        if port_seed is not None:
            port = port_seed + i * 100
        peer_api_uris.append(start_peer(peer_dirs[i], f"{log_file_prefix}peer_0_{i}.log", bootstrap_uri, port=port, extra_env=extra_env, uris_in_env=uris_in_env))

    # Wait for cluster
    wait_for_uniform_cluster_status(peer_api_uris, leader, headers=headers)

    return peer_api_uris, peer_dirs, bootstrap_uri


def make_peer_folder(base_path: Path, peer_number: int) -> Path:
    peer_dir = base_path / f"peer{peer_number}"
    peer_dir.mkdir()
    shutil.copytree("config", peer_dir / "config")
    return peer_dir


def make_peer_folders(base_path: Path, n_peers: int) -> List[Path]:
    peer_dirs = []
    for i in range(n_peers):
        peer_dir = make_peer_folder(base_path, i)
        peer_dirs.append(peer_dir)
    return peer_dirs


def get_cluster_info(peer_api_uri: str, headers={}) -> dict:
    r = requests.get(f"{peer_api_uri}/cluster", headers=headers)
    assert_http_ok(r)
    res = r.json()["result"]
    return res


def print_clusters_info(peer_api_uris: [str], headers={}):
    for uri in peer_api_uris:
        try:
            # do not crash if the peer is not online
            print(json.dumps(get_cluster_info(uri, headers=headers), indent=4))
        except requests.exceptions.ConnectionError:
            print(f"Can't retrieve cluster info for offline peer {uri}")


def fetch_highest_peer_id(peer_api_uris: [str]) -> str:
    max_peer_id = 0
    max_peer_url = None
    for uri in peer_api_uris:
        try:
            # do not crash if the peer is not online
            peer_id = get_cluster_info(uri)['peer_id']
            if peer_id > max_peer_id:
                max_peer_id = peer_id
                max_peer_url = uri
        except requests.exceptions.ConnectionError:
            print(f"Can't retrieve cluster info for offline peer {uri}")
    return max_peer_url


def get_collection_cluster_info(peer_api_uri: str, collection_name: str, headers={}) -> dict:
    r = requests.get(f"{peer_api_uri}/collections/{collection_name}/cluster", headers=headers)
    assert_http_ok(r)
    res = r.json()["result"]
    return res


def get_shard_transfer_count(peer_api_uri: str, collection_name: str) -> int:
    info = get_collection_cluster_info(peer_api_uri, collection_name)
    return len(info["shard_transfers"])


def get_collection_info(peer_api_uri: str, collection_name: str) -> dict:
    r = requests.get(f"{peer_api_uri}/collections/{collection_name}")
    assert_http_ok(r)
    res = r.json()["result"]
    return res


def get_collection_point_count(peer_api_uri: str, collection_name: str, exact: bool = False) -> int:
    r = requests.post(f"{peer_api_uri}/collections/{collection_name}/points/count", json={"exact": exact})
    assert_http_ok(r)
    res = r.json()["result"]["count"]
    return res


def print_collection_cluster_info(peer_api_uri: str, collection_name: str, headers={}):
    print(json.dumps(get_collection_cluster_info(peer_api_uri, collection_name, headers=headers), indent=4))


def get_leader(peer_api_uri: str, headers={}) -> str:
    r = requests.get(f"{peer_api_uri}/cluster", headers=headers)
    assert_http_ok(r)
    return r.json()["result"]["raft_info"]["leader"]


def check_leader(peer_api_uri: str, expected_leader: str, headers={}) -> bool:
    try:
        r = requests.get(f"{peer_api_uri}/cluster", headers=headers)
        assert_http_ok(r)
        leader = r.json()["result"]["raft_info"]["leader"]
        correct_leader = leader == expected_leader
        if not correct_leader:
            print(f"Cluster leader invalid for peer {peer_api_uri} {leader}/{expected_leader}")
        return correct_leader
    except requests.exceptions.ConnectionError:
        # the api is not yet available - caller needs to retry
        print(f"Could not contact peer {peer_api_uri} to fetch cluster leader")
        return False


def leader_is_defined(peer_api_uri: str, headers={}) -> bool:
    try:
        r = requests.get(f"{peer_api_uri}/cluster", headers=headers)
        assert_http_ok(r)
        leader = r.json()["result"]["raft_info"]["leader"]
        return leader is not None
    except requests.exceptions.ConnectionError:
        # the api is not yet available - caller needs to retry
        print(f"Could not contact peer {peer_api_uri} to fetch leader info")
        return False


def check_cluster_size(peer_api_uri: str, expected_size: int, headers={}) -> bool:
    try:
        r = requests.get(f"{peer_api_uri}/cluster", headers=headers)
        assert_http_ok(r)
        peers = r.json()["result"]["peers"]
        correct_size = len(peers) == expected_size
        if not correct_size:
            print(f"Cluster size invalid for peer {peer_api_uri} {len(peers)}/{expected_size}")
        return correct_size
    except requests.exceptions.ConnectionError:
        # the api is not yet available - caller needs to retry
        print(f"Could not contact peer {peer_api_uri} to fetch cluster size")
        return False


def all_nodes_cluster_info_consistent(peer_api_uris: [str], expected_leader: str, headers={}) -> bool:
    expected_size = len(peer_api_uris)
    for uri in peer_api_uris:
        if check_leader(uri, expected_leader, headers=headers) and check_cluster_size(uri, expected_size, headers=headers):
            continue
        else:
            return False
    return True


def all_nodes_have_same_commit(peer_api_uris: [str]) -> bool:
    commits = []
    for uri in peer_api_uris:
        try:
            r = requests.get(f"{uri}/cluster")
            assert_http_ok(r)
            commits.append(r.json()["result"]["raft_info"]["commit"])
        except requests.exceptions.ConnectionError:
            print(f"Could not contact peer {uri} to fetch commit")
            return False
    return len(set(commits)) == 1


def all_nodes_respond(peer_api_uris: [str]) -> bool:
    for uri in peer_api_uris:
        try:
            r = requests.get(f"{uri}/collections")
            assert_http_ok(r)
        except requests.exceptions.ConnectionError:
            print(f"Could not contact peer {uri} to fetch collections")
            return False
    return True


def collection_exists_on_all_peers(collection_name: str, peer_api_uris: [str]) -> bool:
    for uri in peer_api_uris:
        r = requests.get(f"{uri}/collections")
        assert_http_ok(r)
        collections = r.json()["result"]["collections"]
        filtered_collections = [c for c in collections if c['name'] == collection_name]
        if len(filtered_collections) == 0:
            print(
                f"Collection '{collection_name}' does not exist on peer {uri} found {json.dumps(collections, indent=4)}")
            return False
        else:
            continue
    return True


def check_collection_local_shards_count(peer_api_uri: str, collection_name: str,
                                        expected_local_shard_count: int) -> bool:
    return get_collection_local_shards_count(peer_api_uri, collection_name) == expected_local_shard_count


def get_collection_local_shards_count(peer_api_uri: str, collection_name: str) -> int:
    collection_cluster_info = get_collection_cluster_info(peer_api_uri, collection_name)
    return len(collection_cluster_info["local_shards"])


def check_collection_local_shards_point_count(peer_api_uri: str, collection_name: str,
                                            expected_count: int) -> int:
    collection_cluster_info = get_collection_cluster_info(peer_api_uri, collection_name)
    point_count = sum(map(lambda shard: shard["points_count"], collection_cluster_info["local_shards"]))

    is_correct = point_count == expected_count
    if not is_correct:
        print(f"Collection '{collection_name}' on peer {peer_api_uri} ({point_count} != {expected_count}): {json.dumps(collection_cluster_info, indent=4)}")

    return is_correct


def check_collection_shard_transfers_count(peer_api_uri: str, collection_name: str,
                                           expected_shard_transfers_count: int, headers={}) -> bool:
    collection_cluster_info = get_collection_cluster_info(peer_api_uri, collection_name, headers=headers)
    local_shard_count = len(collection_cluster_info["shard_transfers"])
    return local_shard_count == expected_shard_transfers_count


def check_collection_resharding_operations_count(peer_api_uri: str, collection_name: str,
                                           expected_resharding_operations_count: int, headers={}) -> bool:
    collection_cluster_info = get_collection_cluster_info(peer_api_uri, collection_name, headers=headers)

    # TODO(resharding): until resharding release, the resharding operations are not always exposed
    # Once we do release, we can remove the zero fallback here
    # See: <https://github.com/qdrant/qdrant/pull/4599>
    local_resharding_count = len(collection_cluster_info["resharding_operations"]) if "resharding_operations" in collection_cluster_info else 0
    return local_resharding_count == expected_resharding_operations_count


def check_collection_resharding_operation_stage(peer_api_uri: str, collection_name: str, expected_stage: str, headers={}) -> bool:
    collection_cluster_info = get_collection_cluster_info(peer_api_uri, collection_name, headers=headers)
    if "resharding_operations" not in collection_cluster_info:
        return False
    for resharding in collection_cluster_info["resharding_operations"]:
        if "comment" in resharding and resharding["comment"].startswith(expected_stage):
            return True
    return False


def check_collection_shard_transfer_method(peer_api_uri: str, collection_name: str,
                                           expected_method: str) -> bool:
    collection_cluster_info = get_collection_cluster_info(peer_api_uri, collection_name)

    # Check method on each transfer
    for transfer in collection_cluster_info["shard_transfers"]:
        if "method" not in transfer:
            continue
        method = transfer["method"]
        if method == expected_method:
            return True

    return False


def check_collection_shard_transfer_progress(peer_api_uri: str, collection_name: str,
                                             expected_transfer_progress: int,
                                             expected_transfer_total: int) -> bool:
    collection_cluster_info = get_collection_cluster_info(peer_api_uri, collection_name)

    # Check progress on each transfer
    for transfer in collection_cluster_info["shard_transfers"]:
        if "comment" not in transfer:
            continue
        comment = transfer["comment"]

        # Compare progress or total
        current, total = re.search(r"Transferring records \((\d+)/(\d+)\), started", comment).groups()
        if current is not None and expected_transfer_progress is not None and int(
                current) >= expected_transfer_progress:
            return True
        if total is not None and expected_transfer_total is not None and int(total) >= expected_transfer_total:
            return True

    return False


def check_all_replicas_active(peer_api_uri: str, collection_name: str, headers={}) -> bool:
    try:
        collection_cluster_info = get_collection_cluster_info(peer_api_uri, collection_name, headers=headers)
        for shard in collection_cluster_info["local_shards"]:
            if shard['state'] != 'Active':
                return False
        for shard in collection_cluster_info["remote_shards"]:
            if shard['state'] != 'Active':
                return False
    except requests.exceptions.ConnectionError:
        return False
    return True


def check_some_replicas_not_active(peer_api_uri: str, collection_name: str) -> bool:
    return not check_all_replicas_active(peer_api_uri, collection_name)


def check_collection_cluster(peer_url, collection_name):
    res = requests.get(f"{peer_url}/collections/{collection_name}/cluster", timeout=10)
    assert_http_ok(res)
    return res.json()["result"]['local_shards'][0]


def check_strict_mode_enabled(peer_api_uri: str, collection_name: str) -> bool:
    collection_info = get_collection_info(peer_api_uri, collection_name)
    strict_mode_enabled = collection_info["config"]["strict_mode_config"]["enabled"]
    return strict_mode_enabled == True

def check_strict_mode_disabled(peer_api_uri: str, collection_name: str) -> bool:
    collection_info = get_collection_info(peer_api_uri, collection_name)
    strict_mode_enabled = collection_info["config"]["strict_mode_config"]["enabled"]
    return strict_mode_enabled == False

def wait_peer_added(peer_api_uri: str, expected_size: int = 1, headers={}) -> str:
    wait_for(check_cluster_size, peer_api_uri, expected_size, headers=headers)
    wait_for(leader_is_defined, peer_api_uri, headers=headers)
    return get_leader(peer_api_uri, headers=headers)

def wait_for_collection(peer_api_uri: str, collection_name: str):
    def is_collection_listed() -> bool:
        try:
            res = requests.get(f"{peer_api_uri}/collections")
            if not res.ok:
                return False
            collections = set(collection['name'] for collection in res.json()["result"]['collections'])
            return collection_name in collections
        except requests.exceptions.ConnectionError:
            return False

    wait_for(is_collection_listed)

def wait_collection_green(peer_api_uri: str, collection_name: str):
    try:
        wait_for(check_collection_green, peer_api_uri, collection_name)
    except Exception as e:
        print_clusters_info([peer_api_uri])
        raise e


def wait_for_some_replicas_not_active(peer_api_uri: str, collection_name: str):
    try:
        wait_for(check_some_replicas_not_active, peer_api_uri, collection_name)
    except Exception as e:
        print_clusters_info([peer_api_uri])
        raise e


def wait_for_all_replicas_active(peer_api_uri: str, collection_name: str, headers={}):
    try:
        wait_for(check_all_replicas_active, peer_api_uri, collection_name, headers=headers)
    except Exception as e:
        print_collection_cluster_info(peer_api_uri, collection_name, headers=headers)
        raise e


def wait_for_uniform_cluster_status(peer_api_uris: [str], expected_leader: str, headers={}):
    try:
        wait_for(all_nodes_cluster_info_consistent, peer_api_uris, expected_leader, headers=headers)
    except Exception as e:
        print_clusters_info(peer_api_uris)
        raise e


def wait_for_same_commit(peer_api_uris: [str]):
    try:
        wait_for(all_nodes_have_same_commit, peer_api_uris)
    except Exception as e:
        print_clusters_info(peer_api_uris)
        raise e


def wait_all_peers_up(peer_api_uris: [str]):
    try:
        wait_for(all_nodes_respond, peer_api_uris)
    except Exception as e:
        print_clusters_info(peer_api_uris)
        raise e


def wait_for_uniform_collection_existence(collection_name: str, peer_api_uris: [str]):
    try:
        wait_for(collection_exists_on_all_peers, collection_name, peer_api_uris)
    except Exception as e:
        print_clusters_info(peer_api_uris)
        raise e


def wait_for_collection_shard_transfers_count(peer_api_uri: str, collection_name: str,
                                              expected_shard_transfer_count: int, headers={}):
    try:
        wait_for(check_collection_shard_transfers_count, peer_api_uri, collection_name, expected_shard_transfer_count, headers=headers)
    except Exception as e:
        print_collection_cluster_info(peer_api_uri, collection_name, headers=headers)
        raise e


def wait_for_collection_shard_transfer_method(peer_api_uri: str, collection_name: str,
                                              expected_method: str):
    try:
        wait_for(check_collection_shard_transfer_method, peer_api_uri, collection_name, expected_method)
    except Exception as e:
        print_collection_cluster_info(peer_api_uri, collection_name)
        raise e


def wait_for_collection_shard_transfer_progress(peer_api_uri: str, collection_name: str,
                                                expected_transfer_progress: int = None,
                                                expected_transfer_total: int = None):
    try:
        wait_for(check_collection_shard_transfer_progress, peer_api_uri, collection_name, expected_transfer_progress,
                 expected_transfer_total, wait_for_interval=0.1)
    except Exception as e:
        print_collection_cluster_info(peer_api_uri, collection_name)
        raise e


def wait_for_collection_resharding_operations_count(peer_api_uri: str,
                                                    collection_name: str,
                                                    expected_resharding_operations_count:
                                                    int, headers={}, **kwargs):
    try:
        wait_for(check_collection_resharding_operations_count, peer_api_uri, collection_name, expected_resharding_operations_count, headers=headers, **kwargs)
    except Exception as e:
        print_collection_cluster_info(peer_api_uri, collection_name, headers=headers)
        raise e


def wait_for_collection_resharding_operation_stage(peer_api_uri: str, collection_name: str, expected_stage: str, headers={}):
    try:
        wait_for(check_collection_resharding_operation_stage, peer_api_uri, collection_name, expected_stage, headers=headers)
    except Exception as e:
        print_collection_cluster_info(peer_api_uri, collection_name, headers=headers)
        raise e


def wait_for_collection_local_shards_count(peer_api_uri: str, collection_name: str, expected_local_shard_count: int):
    try:
        wait_for(check_collection_local_shards_count, peer_api_uri, collection_name, expected_local_shard_count)
    except Exception as e:
        print_collection_cluster_info(peer_api_uri, collection_name)
        raise e


def wait_for_strict_mode_enabled(peer_api_uri: str, collection_name: str):
    try:
        wait_for(check_strict_mode_enabled, peer_api_uri, collection_name)
    except Exception as e:
        print_collection_cluster_info(peer_api_uri, collection_name)
        raise e

def wait_for_strict_mode_disabled(peer_api_uri: str, collection_name: str):
    try:
        wait_for(check_strict_mode_disabled, peer_api_uri, collection_name)
    except Exception as e:
        print_collection_cluster_info(peer_api_uri, collection_name)
        raise e


def wait_for(condition: Callable[..., bool], *args, wait_for_timeout=WAIT_TIME_SEC, wait_for_interval=RETRY_INTERVAL_SEC, **kwargs):
    start = time.time()
    while not condition(*args, **kwargs):
        elapsed = time.time() - start
        if elapsed > wait_for_timeout:
            raise Exception(
                f"Timeout waiting for condition {condition.__name__} to be satisfied in {wait_for_timeout} seconds")
        else:
            time.sleep(wait_for_interval)

def peer_is_online(peer_api_uri: str, path: str = "/readyz") -> bool:
    try:
        r = requests.get(f"{peer_api_uri}{path}")
        return r.status_code == 200
    except:
        return False


def wait_for_peer_online(peer_api_uri: str, path="/readyz"):
    try:
        wait_for(peer_is_online, peer_api_uri, path=path)
    except Exception as e:
        print_clusters_info([peer_api_uri])
        raise e


def check_collection_green(peer_api_uri: str, collection_name: str, expected_status: str = "green") -> bool:
    collection_cluster_info = get_collection_info(peer_api_uri, collection_name)
    return collection_cluster_info['status'] == expected_status


def check_collection_points_count(peer_api_uri: str, collection_name: str, expected_size: int) -> bool:
    collection_cluster_info = get_collection_info(peer_api_uri, collection_name)
    return collection_cluster_info['points_count'] == expected_size


def wait_collection_points_count(peer_api_uri: str, collection_name: str, expected_size: int):
    try:
        wait_for(check_collection_points_count, peer_api_uri, collection_name, expected_size)
    except Exception as e:
        print_collection_cluster_info(peer_api_uri, collection_name)
        raise e


def wait_collection_on_all_peers(collection_name: str, peer_api_uris: [str], max_wait=30, headers={}):
    # Check that it exists on all peers
    while True:
        exists = True
        for url in peer_api_uris:
            r = requests.get(f"{url}/collections", headers=headers)
            assert_http_ok(r)
            collections = r.json()["result"]["collections"]
            exists &= any(collection["name"] == collection_name for collection in collections)
        if exists:
            break
        else:
            # Wait until collection is created on all peers
            # Consensus guarantees that collection will appear on majority of peers, but not on all of them
            # So we need to wait a bit extra time
            time.sleep(1)
            max_wait -= 1
        if max_wait <= 0:
            raise Exception("Collection was not created on all peers in time")


def wait_collection_exists_and_active_on_all_peers(collection_name: str, peer_api_uris: [str], max_wait=30, headers={}):
    wait_collection_on_all_peers(collection_name, peer_api_uris, max_wait, headers=headers)
    for peer_uri in peer_api_uris:
        # Collection is active on all peers
        wait_for_all_replicas_active(collection_name=collection_name, peer_api_uri=peer_uri, headers=headers)


def create_shard_key(
    shard_key,
    peer_url,
    collection="test_collection",
    shard_number=None,
    replication_factor=None,
    placement=None,
    timeout=10,
    headers={},
):
    r_create = requests.put(
        f"{peer_url}/collections/{collection}/shards?timeout={timeout}",
        json={
            "shard_key": shard_key,
            "shards_number": shard_number,
            "replication_factor": replication_factor,
            "placement": placement,
        },
        headers=headers,
    )
    assert_http_ok(r_create)


def move_shard(source_uri, collection_name, shard_id, source_peer_id, target_peer_id):
    r = requests.post(
        f"{source_uri}/collections/{collection_name}/cluster", json={
            "move_shard": {
                "shard_id": shard_id,
                "from_peer_id": source_peer_id,
                "to_peer_id": target_peer_id
            }
        })
    assert_http_ok(r)

def replicate_shard(source_uri, collection_name, shard_id, source_peer_id, target_peer_id):
    r = requests.post(
        f"{source_uri}/collections/{collection_name}/cluster", json={
            "replicate_shard": {
                "shard_id": shard_id,
                "from_peer_id": source_peer_id,
                "to_peer_id": target_peer_id
            }
        })
    assert_http_ok(r)


def check_data_consistency(data):

    assert(len(data) > 1)

    for i in range(len(data) - 1):
        j = i + 1

        data_i = data[i]
        data_j = data[j]

        if data_i != data_j:
            ids_i = set(x.get("id") for x in data_i)
            ids_j = set(x.get("id") for x in data_j)

            diff = ids_i - ids_j

            if len(diff) < 100:
                print(f"Diff between {i} and {j}: {diff}")
            else:
                sample = list(diff)[:32]
                print(f"Diff len between {i} and {j}: {len(diff)}, sample: {sample}")

            assert False, "Data on all nodes should be consistent"
