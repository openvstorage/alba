"""
Copyright 2015 Open vStorage NV

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

from fabric.api import local, task, warn_only
from fabric.context_managers import lcd, path, show
import os
import time
from env import *
from uuid import uuid4
import json
import socket

os.environ['ALBA_CONFIG'] = './cfg/test.ini'

def _detach(inner, out= '/dev/null'):
    cmd = [
        'nohup',
        ' '.join(inner),
        '>> %s' % out,
        '2>&1',
        '&'
    ]
    return ' '.join(cmd)

def _local_ip():
    #nicked from stack_overflow
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(('8.8.8.8', 0))  # connecting to a UDP address doesn't send packets
    local_ip_address = s.getsockname()[0]
    return local_ip_address

@task
def demo_kill():
    where = local
    with warn_only():
        where("pkill -e -9 %s" % os.path.basename(env['arakoon_bin']))
        where("pkill -e -9 %s" % os.path.basename(env['alba_bin']))
        where("pkill -ef -9 'java.*SimulatorRunner.*'")
        for i in range(N):
            path = env['asd_path_t'] % i
            if os.path.exists(path + "_file"):
                where("sudo umount " + path)
            where("rm -rf %s" % path)
        where("rm -rf /tmp/alba/")
        arakoon_remove_dir()

@task
def arakoon_start_(arakoon_config_file,
                   base_dir,
                   arakoon_nodes):
    where = local

    for node in arakoon_nodes:
        inner = [
            env['arakoon_bin'],
            "--node", node,
            "-config", arakoon_config_file
        ]

        cmd_line = _detach(inner)
        dir_path = "%s/%s" % (base_dir, node)
        where("mkdir -p %s" % dir_path)
        where("ln -fs %s/nsm_host_plugin.cmxs %s/nsm_host_plugin.cmxs" %
              (env['alba_plugin_path'], dir_path))
        where("ln -fs %s/albamgr_plugin.cmxs %s/albamgr_plugin.cmxs" %
              (env['alba_plugin_path'], dir_path))

        where(cmd_line)

@task
def make_ca():
    # CA key & CSR
    root = TLS['root_dir']
    where = local
    where ("mkdir -p %s" % root)
    with lcd(root):
        subject = \
            '/C=BE/ST=Vl-Br/L=Leuven/O=openvstorage.com/OU=AlbaTest/CN=AlbaTest CA'
        where("openssl req -new -nodes -out cacert-req.pem -keyout cacert.key -subj '%s'" % subject)

        # Self-sign CA CSR
        where ('openssl x509 -signkey cacert.key -req -in cacert-req.pem -out cacert.pem')
        where ("rm cacert-req.pem")

@task
def make_cert(name = "arakoon_0", serial = 0 ):
    # CSR
    root = TLS['root_dir']
    node_path = '%s/%s' % (root, name)
    where = local
    where("mkdir -p %s" % node_path)
    with lcd(node_path):
        subject = \
            '/C=BE/ST=Vl-BR/L=Leuven/O=openvstorage.com/OU=AlbaTest/CN=%s' % name
        cmd = "openssl req -out %s-req.pem -new -nodes -keyout %s.key" % (name,name)
        cmd += " -subj '%s'" % subject
        where(cmd)

        # Sign
        cmd = "openssl x509 -req -in %s-req.pem -CA %s/cacert.pem -CAkey %s/cacert.key -out %s.pem" \
              % (name, root, root, name)
        cmd += " -set_serial 0%(serial)d" % {'serial': serial}

        where(cmd)

        where("rm %s-req.pem" % name)

        # Verify
        where('openssl verify -CAfile %(root)s/cacert.pem %(name)s.pem' % \
              {'name': name, 'root':root }
        )

@task
def arakoon_start(tls):
    acf = arakoon_config_file
    where = local
    path = "/tmp/arakoon"
    where ("mkdir -p %s" % path)
    if tls:
        acf = TLS['arakoon_config_file']
        make_ca()
        serial = 0
        for name in arakoon_nodes:
            make_cert(name = name, serial = serial)
            serial += 1
        with lcd(path):
            client = "my_client"
            where("mkdir ./%s" % client)
            make_cert(name = client, serial = serial)

    arakoon_start_(acf, "/tmp/arakoon", arakoon_nodes)

def _extend_arakoon_tls(cmd):
    root = TLS['root_dir']
    name = "my_client"
    cmd.extend([
        '-tls-ca-cert', "%s/cacert.pem" % root,
        '-tls-cert',    "%s/%s/%s.pem"     % (root, name, name),
        '-tls-key',     "%s/%s/%s.key"     % (root, name, name)
    ]
    )
def _extend_alba_tls(cmd):
    root = TLS['root_dir']
    name = "my_client"
    cmd.extend([
        '--tls=%s,%s,%s' % ('%s/cacert.pem' % root,
                            '%s/%s/%s.pem' % (root, name, name),
                            '%s/%s/%s.key' % (root, name, name))
    ])
@task
def arakoon_who_master(arakoon_cfg_file = arakoon_config_file, tls = False):
    if tls:
        arakoon_cfg_file = TLS['arakoon_config_file']

    cmd = [
        env['arakoon_bin'],
        '-config', arakoon_cfg_file,
        '--who-master'
    ]
    if tls:
        _extend_arakoon_tls(cmd)

    where = local
    result = where(' '.join(cmd), capture=True)
    master = result.strip()
    print "master=%s" % result
    return master

@task
def arakoon_remove_dir():
    where = local
    where("rm -rf /tmp/arakoon")

@task
def nsm_host_register(cfg_file, albamgr_cfg = arakoon_config_file, tls = False):
    cmd = [
        env['alba_bin'],
        "add-nsm-host",
        cfg_file,
        "--config",
        albamgr_cfg
    ]
    if tls:
        _extend_alba_tls(cmd)

    cmd_line = ' '.join(cmd)
    where = local
    where(cmd_line)

@task
def nsm_host_register_default(tls = False):
    cfg_file = "%s/cfg/nsm_host_arakoon_cfg.ini" % env['alba_dev']
    albamgr_cfg = arakoon_config_file
    if tls:
        cfg_file = TLS['arakoon_config_file']
        albamgr_cfg = TLS['arakoon_config_file']

    nsm_host_register(cfg_file,
                      albamgr_cfg = albamgr_cfg,
                      tls = tls)

def dump_to_cfg_as_json(cfg_path, obj):
    cfg_content = json.dumps(obj)
    cfg_file = open(cfg_path, 'w')
    cfg_file.write(cfg_content)
    cfg_file.close()

created_loop_devices = False
@task
def create_loop_devices():
    global created_loop_devices
    if not created_loop_devices:
        local("modprobe loop max_loop=64")
        for i in range(30):
            with warn_only():
                local("sudo mknod -m 660 /dev/loop%i b 7 %i" % (i,i))
        created_loop_devices = True

local_nodeid_prefix = str(uuid4())

def _asd_inner(port, path, node_id, slow, multicast):
    global local_nodeid_prefix
    cfg_path = path + "/cfg.json"
    limit = 90 if env['osds_on_separate_fs'] else 99
    cfg = { 'port' : port,
          'node_id' : "%s_%i" % (local_nodeid_prefix, node_id),
          'home' : path,
          'log_level' : 'debug',
          'asd_id' : "%i_%i_%s" % (port,node_id, local_nodeid_prefix),
          'limit' : limit,
          '__sync_dont_use' : False
    }
    if multicast:
        cfg ['multicast'] = 10.0
    else:
        cfg ['multicast'] = None

    dump_to_cfg_as_json(cfg_path, cfg)

    cmd = [
        env['alba_bin'],
        'asd-start',
        "--config", cfg_path
    ]
    if slow:
        cmd.append('--slow')
    return cmd

def _kinetic_inner(port, path):
    cmd =[
        env['kinetic_bin'],
        str(port),
        path,
    ]
    return cmd

@task
def osd_start(port, path, node_id, kind, slow, setup_dir=True, multicast = True):
    where = local
    if setup_dir:
        where("mkdir -p %s" % path)

        if env['osds_on_separate_fs']:
            create_loop_devices()
            volume_file = path + "_file"
            where("fallocate -l 200000000 %s" % volume_file)
            where("mke2fs -t ext4 -F %s" % volume_file)
            where("sudo mount -o loop,async %s %s" % (volume_file, path))
            where("sudo chown -R $USER %s" % path)

    inner = None
    if kind == "ASD":
        inner = _asd_inner(port, path,
                           node_id, slow and port == 8000,
                           multicast)
    else:
        inner = _kinetic_inner(port, path)

    cmd_line = _detach(inner, out = "%s/output" % path)
    where(cmd_line)


@task
def osd_stop(port):
    cmd = ["fuser -k -n tcp %s" % port]
    where = local
    cmd_line = ' '.join(cmd)
    where(cmd_line)


@task
def create_namespace(namespace, abm_cfg = arakoon_config_file, tls = False):
    cmd = [
        env['alba_bin'],
        "create-namespace",
        namespace,
        '--config',
        abm_cfg
    ]
    if tls:
        alba._extend_alba_tls(cmd)

    cmd_line = ' '.join(cmd)
    where = local
    where(cmd_line)

@task
def create_namespace_demo(tls = False):
    create_namespace(namespace = namespace, tls = tls)


@task
def arakoon_stop():
    where = local
    for node in arakoon_nodes:
        cmd = [
            "pkill",
            "--list-name",
            "-f",
            "'^%s --node %s'" % (env['arakoon_bin'],node),
        ]
        cmd_line = ' '.join(cmd)
        where(cmd_line)

def _add_tls_config(cfg):
    client_tls = (TLS['root_dir'], 'my_client', 'my_client')
    cfg['tls_client'] = {
        'ca_cert' : '%s/cacert.pem' % TLS['root_dir'],
        #creds is optional...
        'creds' : ('%s/%s/%s.pem' % client_tls, '%s/%s/%s.key' % client_tls)
    }

@task
def proxy_start(abm_cfg = arakoon_config_file,
                proxy_id = '0',
                n_proxies = 1,
                n_others = 1,
                tls = False):
    proxy_id = int(proxy_id) # how to enter ints from cli?
    ssl = bool(tls)

    proxy_home = "/tmp/alba/proxies/%02i" % proxy_id
    proxy_cache = proxy_home + "/fragment_cache"
    proxy_cfg = "%s/proxy.cfg" % proxy_home

    where = local
    inner = [
        env['alba_bin'],
        "proxy-start",
        "--config=%s" % proxy_cfg
    ]
    local("mkdir -p %s" % proxy_home)

    # make a proxy local copy of albamgr cfg file, as it will rewrite it
    proxy_albmamgr_cfg_file = proxy_home + "/albamgr.cfg"
    local("cp %s %s" % (abm_cfg, proxy_albmamgr_cfg_file))
    chattiness = 1.0 / (n_proxies + n_others)
    chattiness = round(chattiness, 2)
    with lcd(proxy_home):
        cfg = {
            'port' : 10000 + proxy_id,
            'albamgr_cfg_file' : proxy_albmamgr_cfg_file,
            'log_level' : 'debug',
            'fragment_cache_dir' : proxy_cache,
            'manifest_cache_size' : 100 * 1000,
            'fragment_cache_size' : 100 * 1000 * 1000,
            'chattiness': chattiness
        }
        if tls:
            _add_tls_config(cfg)

        dump_to_cfg_as_json(proxy_cfg, cfg)

        where("mkdir -p " + proxy_cache)
        cmd_line = _detach(inner, out = '%s/proxy.out' % proxy_home)
        where(cmd_line)

@task
def proxy_stop(proxy_id = 0):
    port = 10000 + proxy_id
    where = local
    where("fuser -k -n tcp %i" % port)


@task
def maintenance_start(abm_cfg = arakoon_config_file,
                      n_agents = 1, n_others = 1,
                      tls = False):
    maintenance_home = "/tmp/alba/maintenance"

    local("mkdir -p %s" % maintenance_home)

    maintenance_albmamgr_cfg_file = maintenance_home + "/albamgr.cfg"
    local("cp %s %s" % (abm_cfg, maintenance_albmamgr_cfg_file))
    with lcd(maintenance_home):
        maintenance_cfg = "%s/maintenance.cfg" % maintenance_home
        cfg = { 'albamgr_cfg_file' : maintenance_albmamgr_cfg_file,
                'log_level' : 'debug'
        }

        if tls:
            _add_tls_config(cfg)

        dump_to_cfg_as_json(maintenance_cfg, cfg)


        for i in range(n_agents):
            where = local
            inner = [
                env['alba_bin'],
                'maintenance',
                "--config=%s" % maintenance_cfg
            ]
            out = '%s/maintenance_%i_%i.out' % (maintenance_home,
                                                i,
                                                n_agents)
            cmd_line = _detach(inner, out = out )
            where(cmd_line)

@task
def maintenance_stop():
    inner = [
        'pkill',
        '-f',
        "'^%s maintenance'" % env['alba_bin']
    ]
    cmd_line = ' '.join(inner)
    where = local
    where(cmd_line)

def wait_for_master(arakoon_cfg_file = arakoon_config_file, tls = False, max = 10):
    waiting = True
    count = 0
    m = None
    while waiting:

        try:
            m = arakoon_who_master(arakoon_cfg_file,
                                   tls = tls)

        except:
            m = None
        if m<>None:
            waiting = False
        if count == max:
            raise Exception("this should not take so long")
        count = count + 1
        time.sleep(1)

@task
def claim_local_osds(n, multicast=True, tls = False):
    print "claim_local_osds(%i, multicast=%s, tls=%s)" % (n,multicast, tls)

    global local_nodeid_prefix
    global claimed_osds
    claimed_osds = 0
    my_ip = _local_ip()
    def is_local(osd):
        node_id = osd['node_id']
        if node_id.startswith(local_nodeid_prefix):
            return True
        if my_ip in osd['ips']:
            # this might claim osds from other tests on same machine
            # might be a future problem on jenkins
            return True
        return False

    def inner():
        abm_cfg = arakoon_config_file

        if tls:
            abm_cfg = TLS['arakoon_config_file']

        global claimed_osds
        cmd = [env['alba_bin'],
               'list-available-osds',
               '--config', abm_cfg,
               '--to-json']
        if tls:
            _extend_alba_tls(cmd)

        cmd.extend(['2>',  '/dev/null'])

        cmd_line = ' '.join(cmd)
        output = local(cmd_line, capture=True)

        osds = json.loads(output)['result']
        print osds
        print len(osds)

        for osd in osds:
            if is_local(osd):
                cmd = [env['alba_bin'], 'claim-osd',
                       '--config', abm_cfg,
                       '--long-id', osd['long_id'], '--to-json']
                if tls:
                    _extend_alba_tls(cmd)
                cmd_line = ' '.join(cmd)
                local(cmd_line)
                claimed_osds += 1

    count = 0
    while ((claimed_osds != n) and (count < 60)):
        inner()
        count += 1
        time.sleep(1)

    assert (claimed_osds == n)

@task
def start_osds(kind, n, slow, multicast=True):
    n = int(n) # as a separate task, you will be getting a string
    for i in range(n):
        if kind == "MIXED":
            my_kind = KINDS[i % 2]
        else:
            my_kind = kind

        path = env['asd_path_t'] % i
        port = 8000 + i
        node_id = int (port >> 2)
        osd_start(port = port,
                  path = path,
                  node_id = node_id,
                  kind = my_kind,
                  slow = slow,
                  multicast = multicast)


@task
def demo_setup(kind = default_kind,
               multicast = True,
               n_agents = 1,
               n_proxies = 1,
               tls = 'False'):
    tls = eval(tls) # bool('False') == True
    print tls

    cmd = [ env['arakoon_bin'], '--version' ]
    local(' '.join(cmd))
    cmd = [ env['alba_bin'], 'version' ]
    local(' '.join(cmd))

    arakoon_start(tls = tls)
    wait_for_master(tls = tls)

    n_agents = int(n_agents)
    n_proxies = int(n_proxies)

    proxy_start(n_proxies = n_proxies,
                n_others = n_agents,
                tls = tls)


    maintenance_start(n_agents = n_agents,
                      n_others = n_proxies,
                      tls = tls
    )

    nsm_host_register_default(tls = tls)

    start_osds(kind, N, True, multicast)

    claim_local_osds(N, multicast, tls = tls)

    create_namespace_demo(tls = tls)



@task
def smoke_test(sudo = False):
    m = arakoon_who_master()
    print "master:", m
    fuser = "fuser"
    centos = False
    try:
        with open('/etc/os-release','r') as f:
            data = f.read()
            print data
            if data.find('centos') > 0:
                centos = True
    except:
        pass

    if centos:
        print "we're on centos"
        sudo = True

    my_fuser = "fuser"

    if sudo:
        my_fuser = "sudo fuser"

    def how_many_osds():
        cmd = "%s -n tcp " % my_fuser
        for i in range(N):
            cmd += " %i" % (8000 + i)
        n = 0
        with warn_only():
            r = local( cmd , capture=True )
            print "r='%s'" % r
            processes = r.split()
            print processes
            n = len(processes)
        return n

    n = how_many_osds()

    print "found %i osds" % n
    if (n < N):
        raise Exception("only %i OSD running, need %i" % (n, N))
    def proxy_running():
        cmd = "%s -n tcp %i | wc" % (fuser,10000)
        r = int(local(cmd, capture = True).split()[-2])
        return r
    if not proxy_running():
        raise Exception("proxy not running")
    else:
        print "proxy running..."


def dump_junit_xml():
    from junit_xml import TestSuite, TestCase

    test_cases = [TestCase('testname', 'package.test', 123.345, 'I am stdout!', 'I am stderr!')]
    ts = [ TestSuite("stress test suite", test_cases) ]

    with open('./testresults.xml', mode='w') as f:
        TestSuite.to_file(f, ts)

def _alba_package(alba_version, alba_revision):
    alba_suffix = ""
    if alba_revision <> "":
        alba_suffix = "-%s" % alba_revision

    alba_package = "%s%s" % (alba_version, alba_suffix)
    return alba_package
@task
def deb_integration_test(arakoon_version,
                         alba_version,
                         alba_revision = "",
                         xml=False):

    alba_package = _alba_package(alba_version, alba_revision)
    local("sudo dpkg -r arakoon | true")
    local("sudo dpkg -r alba | true")
    local("rm -f arakoon_%s_amd64.deb | true" % arakoon_version)
    local("rm -f alba_%s_amd64.deb | true" % alba_package)

    local("wget https://github.com/openvstorage/arakoon/releases/download/%s/arakoon_%s_amd64.deb" % (arakoon_version,arakoon_version))
    local("sudo gdebi -n arakoon_%s_amd64.deb" % arakoon_version)
    local("wget http://10.100.129.100:8080/view/alba/job/alba_docker_deb_from_github/lastSuccessfulBuild/artifact/alba_%s_amd64.deb" % alba_package)
    local("sudo gdebi -n alba_%s_amd64.deb" % alba_package)

    demo_kill()

    env['arakoon_bin'] = '/usr/bin/arakoon'
    env['alba_bin'] = '/usr/bin/alba'
    env['alba_plugin_path'] = '/usr/lib/alba'

    demo_kill()
    demo_setup()

    cmd = [
        env['alba_bin'],
        'list-namespaces',
        '--to-json',
        '2> /dev/null' ]
    local(' '.join(cmd))

    for i in range(N):
        local("alba asd-statistics -h ::1 -p %i" % (8000+i))

    # TODO: why doesn't this work?
    #with show('debug'):
    #    smoke_test(sudo=True)

    demo_kill()

    local("sudo dpkg -r arakoon")
    local("sudo dpkg -r alba")

    if xml:
        dump_junit_xml()

@task
def rpm_integration_test(arakoon_version,
                         alba_version,
                         xml=False):

    if not os.path.exists("/etc/redhat-release"):
        raise Exception("should be run on redhat")
    alba_package = _alba_package(alba_version, "1")

    local("sudo yum -y erase arakoon | true")
    local("sudo yum -y erase alba | true")

    arakoon_rpm = "arakoon-%s-3.el7.centos.x86_64.rpm" % arakoon_version
    alba_rpm = "alba-%s.el7.centos.x86_64.rpm" % alba_package
    local("rm -f %s" % arakoon_rpm)
    local("wget https://github.com/openvstorage/arakoon/releases/download/%s/%s" % (arakoon_version, arakoon_rpm))
    local("rm -f %s" % alba_rpm)
    local("wget http://10.100.129.100:8080/view/alba/job/alba_docker_rpm_from_github/lastSuccessfulBuild/artifact/rpmbuild/RPMS/x86_64/%s" % (alba_rpm,))


    local("sudo yum -y erase arakoon | true")
    local("sudo yum -y erase alba | true")
    local("sudo yum -y --nogpgcheck localinstall %s" % arakoon_rpm)
    local("sudo yum -y --nogpgcheck localinstall %s" % alba_rpm)

    env['arakoon_bin'] = '/bin/arakoon'
    env['alba_bin'] = '/bin/alba'
    env['alba_plugin_path'] = '/usr/lib64/alba'
    demo_kill()
    demo_setup()

    cmd = [
        env['alba_bin'],
        'list-namespaces',
        '--config=./cfg/test.ini',
        '--to-json',
        '2> /dev/null' ]
    local(' '.join(cmd))
    print "got here..."
    #with path('/sbin'): # for CentOS
    #    smoke_test()
    #
    for i in range(N):
        local("alba asd-statistics -h ::1 -p %i" % (8000+i))
    demo_kill()

    local("sudo yum -y erase arakoon alba")
    if xml:
        dump_junit_xml()
