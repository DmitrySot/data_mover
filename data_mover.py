# Copyright IBM Corp. 2016, 2016 All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#--------------------------------------------------------------------
# Written By Dmitry Sotnikov (dmitrys@il.ibm.com)
#

"""
This script assumes:
1. ssh keys distributed across the cluster
2. the drivers are mapped at the same way on all the machines
3. currently it only distributes the partition to the right disks,
   without the last step of moving it to the object directory.
"""

from swift.common.ring import RingData
from optparse import OptionParser, OptionGroup

from sets import Set
import os
import json

import time
import random
from os.path import isfile, join

from eventlet import GreenPool, Timeout
from eventlet.green import subprocess
from eventlet.support.greenlets import GreenletExit

from swift.common.utils import whataremyips, get_logger, \
    config_true_value, ismount, unlink_older_than, rsync_ip

from swift.obj.diskfile import (DiskFileManager, get_data_dir,
                                get_tmp_dir)

from swift.common.storage_policy import POLICIES, REPL_POLICY
from swift.common.daemon import Daemon
from swift.common.ring.utils import is_local_device


DEFAULT_DUMP_FILE = "moving_map_dump.txt"


def device_dict(ring_data):
    """
    Create a dictionary that contains the devices from the ring structure
    The dictionary key is the device ID, the value is the
    (device ip address, device name) pair

    :param ring_data: ring dat structure

    :returns: dictionary that contains the devices from the ring structure
              the dictionary maps the device ID to its node ip and name
    """

    ret_dict = {}
    for device in ring_data.to_dict()['devs']:
        if device is None:
            continue
        ret_dict[str(device['id'])] = (device['ip'], device['device'])

    return ret_dict


def build_moving_map(old_ring_data, new_ring_data, bound=float('inf')):
    """
    Create a moving map structure, that represents the data moving plan
    For each partition that should be moved this structure
    defines the source_set and destination_set devices
    The amount of partition that would be scanned during this process
    can be bounded by bound parameter,
    at this case the moving map would be partial,
    and would not represent all the required changes

    :param old_ring_data: ring date structure that represents the old ring
    :param new_ring_data: ring date structure that represents the new ring
    :param bound: bounds the amount of partitions that would be scanned

    :returns: the data moving map structure
    """

    ret_dict = {}
    # a, b, c are lists of the devices that hold the first,second, and third
    # replicas from the old ring.
    # e.g. a[partition_id] represents the ID of the device
    # that holds the first replica of the "partition_id" partition
    # based on the old ring
    a, b, c = old_ring_data.to_dict()['replica2part2dev_id']

    # x, y, z are lists of the devices that hold the first,second, and third
    # replicas from the new ring.
    # e.g. x[partition_id] represents the ID of the device
    # that holds the first replica of the "partition_id" partition
    # based on the new ring
    x, y, z = new_ring_data.to_dict()['replica2part2dev_id']

    array_size = min(bound, len(x))

    for partition_id in range(array_size):
        # old_devices - the set of the devices that hold all the replicas
        # of partition_id based on the old ring
        old_devices = Set([a[partition_id], b[partition_id], c[partition_id]])
        # new_devices - the set of the devices that hold all the replicas
        # of partition_id based on the new ring
        new_devices = Set([x[partition_id], y[partition_id], z[partition_id]])

        # source_set - set of devices that no more hold the partition_id
        source_set = old_devices.difference(new_devices)
        # destination_set - set of devices that should get the partition_id
        # from the source_set devices
        destination_set = new_devices.difference(old_devices)

        if len(source_set) != len(destination_set):
            raise Exception("len(source_set) != len(destination_set)")

        while len(source_set) > 0:
            source_device = source_set.pop()
            destination_device = destination_set.pop()

            # partition_id is stored on multiple devices,
            # to allow a fast check if a given device should
            # care for a given partition
            # the pair (source_device, partition_id) will be used
            # as a key of the mover map dictionary
            # to make it human readable it will be encoded as string
            # "<device_id>_<partition_id>".
            # the dictionary value would be a triplet
            # partition_id, source_device_id, destination_device_id
            # this information would be used for creation of job
            # for partition movement.

            ret_dict["%s_%s" % (source_device, partition_id)] =\
                (map(str, [partition_id, source_device, destination_device]))

    return ret_dict


def print_moving_map(old_ring_data, new_ring_data, bound=float('inf')):
    """
    The function create a moving map structure, based on the old and new
    ring data structures, and prints it out

    :param old_ring_data: ring date structure that represents the old ring
    :param new_ring_data: ring date structure that represents the new ring
    :param bound: bounds the amount of partitions that would be scanned
    """

    old_ring_dict = device_dict(old_ring_data)
    new_ring_dict = device_dict(new_ring_data)
    moving_map = build_moving_map(old_ring_data, new_ring_data, bound)

    for key in moving_map:
        partition_id, source_device_id, destination_device_id = moving_map[key]
        print (partition_id,
               old_ring_dict[source_device_id],
               new_ring_dict[destination_device_id])


def dump_moving_map(old_ring_data, new_ring_data,
                    file_name=DEFAULT_DUMP_FILE):
    """
    The function dumps the moving map structure created from old and new ring
    data structures in to the file

    :param old_ring_data: ring date structure that represents the old ring
    :param new_ring_data: ring date structure that represents the new ring
    :param file_name: the moving map dump file name
    """

    old_ring_dict = device_dict(old_ring_data)
    new_ring_dict = device_dict(new_ring_data)
    moving_map = build_moving_map(old_ring_data, new_ring_data)

    with open(file_name, "w") as f:
        f.write(json.dumps({"old_device_dict": old_ring_dict,
                            "new_device_dict": new_ring_dict,
                            "moving_map": moving_map}))


def load_moving_map(file_name=DEFAULT_DUMP_FILE, test_mode=False):
    """
    The function loads the moving map structure from the dump file

    :param file_name: the moving map dump file name
    :param test_mode: controls the printouts for the testing

    :returns: the triple that contains:
    old_ring_dict: the dictionary with devices from old ring,
    new_ring_dict the dictionary with devices from new ring,
    moving_map: the data moving map structure
    """

    if not os.path.isfile(file_name):
        raise Exception('the wrong path to the dump file specified.')

    with open(file_name, "r") as f:
        input_data = json.loads(f.read())

    old_ring_dict = input_data["old_device_dict"]
    new_ring_dict = input_data["new_device_dict"]
    moving_map = input_data["moving_map"]

    if test_mode:

        counter = 15
        for key in moving_map:

            partition, source_device, destination_device = moving_map[key]
            print (partition,
                   old_ring_dict[source_device],
                   new_ring_dict[destination_device])

            counter -= 1
            if counter == 0:
                break

    return old_ring_dict, new_ring_dict, moving_map


def validate_moving_map_options(options, args):
    '''
    Validates the input parameters
    '''

    if options.run == "False":
        options.run = ""

    o = False
    n = False
    r = False
    if options.old_ring_path:
        o = True
    if options.new_ring_path:
        n = True
    if options.run and options.run != "":
        r = True

    if r is True and (o is True or n is True):
        raise Exception('-r could not be used with -o and -n parameters.')

    if (o != n):
        raise Exception(
            'both old and new ring should be specified together.')

    if options.test == "False":
        options.test = ""

    if options.run == "False":
        options.run = ""


class ObjectMover(Daemon):
    def __init__(self, conf):
        """
        :param conf: configuration object obtained from ConfigParser
        :param logger: logging object
        """

        self.conf = conf
        self.logger = get_logger(conf, log_route='object-mover')
        self.devices_dir = conf.get('devices', '/srv/node')
        self.mount_check = config_true_value(conf.get('mount_check', 'true'))
        self.vm_test_mode = config_true_value(conf.get('vm_test_mode', 'no'))
        self.swift_dir = conf.get('swift_dir', '/etc/swift')
        self.bind_ip = conf.get('bind_ip', '0.0.0.0')
        self.servers_per_port = int(conf.get('servers_per_port', '0') or 0)
        self.port = None if self.servers_per_port else \
            int(conf.get('bind_port', 6000))
        self.concurrency = int(conf.get('concurrency', 1))
        self.reclaim_age = int(conf.get('reclaim_age', 86400 * 7))

        self.handoffs_first = config_true_value(conf.get('handoffs_first',
                                                         False))

        self.data_moving_map_dump = (conf.get('data_moving_map_dump')
                                     or DEFAULT_DUMP_FILE)

        self._diskfile_mgr = DiskFileManager(conf, self.logger)

        self.mover_tmp_dir = (conf.get('mover_tmp_dir') or 'data_mover')
        self.retries = int(conf.get('retries', 3))
        self.test = bool(conf.get('test', False))

        self.retrie_list = []

    def create_remote_directory(self, job):
        """
        Creates a temporal directory, at remote server.

        :param job: information about the partition being synced

        """
        node = job['node']

        args = ["ssh", rsync_ip(node['replication_ip']),
                "mkdir", "-p", job['remote_path']]

        if not self.test:
            proc = subprocess.Popen(args,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT)

            results = proc.stdout.read()
            ret_val = proc.wait()

            #TODO: ret_val check
            (results, ret_val)

        else:
            print " ".join(args)

    #TODO: same as replicator load_object_ring
    def load_object_ring(self, policy):
        """
        Make sure the policy's rings are loaded.

        :param policy: the StoragePolicy instance
        :returns: appropriate ring object
        """

        policy.load_ring(self.swift_dir)
        return policy.object_ring

    #TODO: check if _rsync from replicator will be used instead
    def _rsync(self, args):
        """
        Execute the rsync binary to replicate a partition.

        :returns: return code of rsync process. 0 is successful
        """

        start_time = time.time()
        ret_val = None

        proc = subprocess.Popen(args,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT)

        results = proc.stdout.read()
        ret_val = proc.wait()

        total_time = time.time() - start_time
        for result in results.split('\n'):
            if result == '':
                continue
            if result.startswith('cd+'):
                continue
            if not ret_val:
                self.logger.info(result)
            else:
                self.logger.error(result)
        if ret_val:
            error_line = 'Bad rsync return code: %(ret)d <- %(args)s' % \
                {'args': str(args), 'ret': ret_val}
            if self.rsync_error_log_line_length:
                error_line = error_line[:self.rsync_error_log_line_length]
            self.logger.error(error_line)
        elif results:
            self.logger.info(
                "Successful rsync of %(src)s at %(dst)s (%(time).03f)",
                {'src': args[-2], 'dst': args[-1], 'time': total_time})
        else:
            self.logger.debug(
                "Successful rsync of %(src)s at %(dst)s (%(time).03f)",
                {'src': args[-2], 'dst': args[-1], 'time': total_time})

        return ret_val

    def rsync(self, job):
        """
        Uses rsync to implement the sync method. This was the first
        sync method in Swift.
        """

        if not os.path.exists(job['path']):
            if self.test:
                print "Error: the path %s does not exists" % job['path']
            return False, {}

        args = [
            'rsync',
            '-a',
            '--whole-file',
            '--human-readable',
            '--xattrs',
            '--ignore-existing',
        ]

        node = job['node']
        node_ip = rsync_ip(node['replication_ip'])
        rsync_module = '%s:%s' % (node_ip, job['remote_path'])

        args.append(job['path'])
        args.append(rsync_module)

        if not self.test:
            return self._rsync(args) == 0, {}
        else:
            print " ".join(args)
            return True, {}

    def update(self, job):
        """
        High-level method that replicates a single partition.

        :param job: a dict containing info about the partition to be replicated
        """

        self.logger.increment('partition.update.count.%s' % (job['device'],))

        begin = time.time()
        try:

            self.create_remote_directory(job)
            success, _junk = self.rsync(job)
            if not success:
                self.retrie_list.append(job)

        except (Exception, Timeout):
            self.logger.exception("Error syncing partition")
        finally:
            self.partition_times.append(time.time() - begin)
            self.logger.timing_since('partition.update.timing', begin)

#TODO: same as replicator kill coros
    def kill_coros(self):
        """Utility function that kills all coroutines currently running."""
        for coro in list(self.run_pool.coroutines_running):
            try:
                coro.kill(GreenletExit)
            except GreenletExit:
                pass

    def build_replication_jobs(self, policy, ips, old_dict,
                               new_dict, moving_map):
        """
        Helper function for collect_jobs to build jobs for replication
        using replication style storage policy

        :param policy: swift policy object
        :param ips: the local server ips
        :param old_dict: dictionary with devices from old ring
        :param new_dict: dictionary with devices from new ring
        :param moving_map: the dictionary that contains all the partitions
            that should be moved, their sources and destinations
        """

        jobs = []
        data_dir = get_data_dir(policy)
        devices = Set(map(lambda x: x[1], moving_map.values()))
        partitions = Set(map(lambda x: x[0], moving_map.values()))

        for local_dev in [dev for dev in policy.object_ring.devs
                          if (dev
                              and is_local_device(ips,
                                                  self.port,
                                                  dev['replication_ip'],
                                                  dev['replication_port'])
                              )]:

            if self.test:
                print local_dev['id']

            if unicode(local_dev['id']) not in devices:
                continue

            dev_path = join(self.devices_dir, local_dev['device'])
            obj_path = join(dev_path, data_dir)
            tmp_path = join(dev_path, get_tmp_dir(policy))
            if self.mount_check and not ismount(dev_path):
                self.logger.warn('%s is not mounted' % local_dev['device'])
                continue
            unlink_older_than(tmp_path, time.time() - self.reclaim_age)

            for partition in os.listdir(obj_path):
                partition = unicode(partition)

                if (partition not in partitions):
                    continue

                try:

                    key = "%s_%s" % (local_dev['id'], partition)
                    if key not in moving_map:
                        continue

                    job_path = join(obj_path, partition)

                    _, source_id, dest_id = moving_map[key]

                    if source_id != unicode(local_dev['id']):
                        continue

                    node = {}
                    replication_ip, replication_device = new_dict[dest_id]
                    node['replication_ip'] = replication_ip
                    node['device'] = replication_device

                    remote_path = os.path.join(self.devices_dir,
                                               node['device'],
                                               self.mover_tmp_dir)

                    jobs.append(
                        dict(path=job_path,
                             device=local_dev['device'],
                             obj_path=obj_path,
                             node=node,
                             policy=policy,
                             partition=partition,
                             remote_path=remote_path))

                except ValueError:
                    continue
                except Exception as e:
                    self.logger.exception(
                        "an %s exception accure at build_replication_jobs" % e)
                    if self.test:
                        print e
        return jobs

    def collect_jobs(self, old_dict, new_dict, moving_map):
        """
        Returns a sorted list of jobs (dictionaries) that specify the
        partitions, nodes, etc to be rsynced.

        :param old_dict: dictionary with devices from old ring
        :param new_dict: dictionary with devices from new ring
        :param moving_map: the dictionary that contains all the partitions
            that should be moved, their sources and destinations
        """

        jobs = []
        ips = whataremyips(self.bind_ip)

        for policy in POLICIES:
            if policy.policy_type == REPL_POLICY:
                # ensure rings are loaded for policy
                self.load_object_ring(policy)
                jobs += self.build_replication_jobs(
                    policy, ips, old_dict, new_dict, moving_map)
        random.shuffle(jobs)

        return jobs

    def move(self, old_dict, new_dict, moving_map):
        """Run a move pass.

        :param old_dict: dictionary with devices from old ring
        :param new_dict: dictionary with devices from new ring
        :param moving_map: the dictionary that contains all the partitions
            that should be moved, their sources and destinations
        """

        self.start = time.time()
        self.replication_count = 0
        self.last_replication_count = -1
        self.partition_times = []

        try:
            self.run_pool = GreenPool(size=self.concurrency)
            jobs = self.collect_jobs(old_dict, new_dict, moving_map)
            for job in jobs:
                dev_path = join(self.devices_dir, job['device'])
                if self.mount_check and not ismount(dev_path):
                    self.logger.warn('%s is not mounted' % job['device'])
                    continue

                try:
                    if isfile(job['path']):
                        # Clean up any (probably zero-byte) files where a
                        # partition should be.
                        self.logger.warning(
                            'Removing partition directory '
                            'which was a file: %s', job['path'])
                        os.remove(job['path'])
                        continue
                except OSError:
                    continue

                self.run_pool.spawn(self.update, job)

            self.run_pool.waitall()

        except (Exception, Timeout) as e:
            self.kill_coros()
            self.logger.exception(
                "Exception in top-level partition move loop %s" % e)
            if self.test:
                print e

    def run_once(self, *args, **kwargs):
        start = time.time()
        self.logger.info("Running object mover in script mode.")

        old_dict, new_dict, moving_map =\
            load_moving_map(self.data_moving_map_dump)

        self.move(old_dict, new_dict, moving_map)

        trie = 0
        while trie < self.retries:
            if len(self.retrie_list) == 0:
                break
            current_retrie_list = self.retrie_list
            self.retrie_list = []

            for job in current_retrie_list:
                self.update(job)

            trie += 1

        total = (time.time() - start) / 60
        self.logger.info(
            "Object move complete (once). "
            "(%.02f minutes), %s partition movement failed"
            % (total, len(self.retrie_list)))


def main():

    usage = "usage: %prog [options] arg"
    parser = OptionParser(usage)

    group = OptionGroup(parser, "Moving Map Building")
    group.add_option("-o", "--old_ring", dest="old_ring_path",
                     help="The path to the old ring file.")
    group.add_option("-n", "--new_ring", dest="new_ring_path",
                     help="The path to the new ring file.")
    group.add_option("-t", "--test", dest="test", default="False",
                     help="Controls whether the execution will only printout "
                     "the commands to standard output, "
                     "without explicitly run them. To set (True) "
                     "to unset (False). The default value is False. "
                     "Used for manual testing only.")
    group.add_option("-r", "--run", dest="run", default="False",
                     help="Controls the data movement script execution. "
                     " Default is False")
    group.add_option("-f", "--file", dest="dump_file",
                     help="The path to the data moving map dump file.")
    group.add_option("-c", "--concurrency", dest="concurrency", type="int",
                     default=1,
                     help="The concurrency level, the default is 1.")

    group.add_option("-m", "--mover_tmp_dir", dest="mover_tmp_dir",
                     default='data_mover',
                     help="The name of temporal directory that would "
                     "be used for data migration")

    parser.add_option_group(group)

    (options, args) = parser.parse_args()
    validate_moving_map_options(options, args)

    if options.dump_file:
        dump_file = options.dump_file
    else:
        dump_file = DEFAULT_DUMP_FILE

    if bool(options.run):
        conf = {}

        conf['data_moving_map_dump'] = dump_file
        conf['concurrency'] = options.concurrency
        conf['test'] = options.test
        conf['mover_tmp_dir'] = options.mover_tmp_dir

        objMover = ObjectMover(conf=conf)
        objMover.run_once()

    else:
        old_ring_data = RingData.load(options.old_ring_path)
        new_ring_data = RingData.load(options.new_ring_path)

        if bool(options.test):
            print_moving_map(old_ring_data, new_ring_data, 10)

        dump_moving_map(old_ring_data, new_ring_data, dump_file)

        if bool(options.test):
            load_moving_map(dump_file, True)


if __name__ == "__main__":
    main()
