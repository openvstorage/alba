"""
Copyright (C) 2016 iNuron NV

This file is part of Open vStorage Open Source Edition (OSE), as available from


    http://www.openvstorage.org and
    http://www.openvstorage.com.

This file is free software; you can redistribute it and/or modify it
under the terms of the GNU Affero General Public License v3 (GNU AGPLv3)
as published by the Free Software Foundation, in version 3 as it comes
in the <LICENSE.txt> file of the Open vStorage OSE distribution.

Open vStorage is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY of any kind.
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
def demo_kill(env = env):
    where = local
    with warn_only():
        where("pkill -e -9 %s" % os.path.basename(env['arakoon_bin']))
        where("pkill -e -9 %s" % os.path.basename(env['alba_bin']))
        where("pkill -ef -9 'java.*SimulatorRunner.*'")
        where("fuser -k -f %s" % env['monitoring_file'])
        for i in range(N):
            path = env['asd_path_t'] % i
            if os.path.exists(path + "_file"):
                where("sudo umount " + path)
            where("rm -rf %s" % path)
        where("rm -rf %s" % ALBA_BASE_PATH)
        arakoon_remove_dir()



@task
def arakoon_start_(arakoon_config_file,
                   base_dir,
                   arakoon_nodes, env = env):
    where = local

    for node in arakoon_nodes:
        inner = [
            env['arakoon_bin'],
            "--node", node,
            "-config", arakoon_config_file
        ]


        dir_path = "%s/%s" % (base_dir, node)
        cmd_line = _detach(inner, out = "%s/out" % dir_path)
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
def make_cert(name, serial = None):
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

        if serial:
            cmd += " -set_serial 0%(serial)d" % {'serial': serial}

        # persist serial number state
        cmd += " -CAcreateserial -CAserial %s/cacert-serial.seq" % root


        where(cmd)

        where("rm %s-req.pem" % name)

        # Verify
        where('openssl verify -CAfile %(root)s/cacert.pem %(name)s.pem' % \
              {'name': name, 'root':root }
        )

@task
def arakoon_start(acf = None, env = env):

    where = local
    path = ARAKOON_PATH
    where ("mkdir -p %s" % path)
    tls = env['alba_tls']

    if is_true(tls):
        acf = TLS['arakoon_config_file']
        make_ca()
        serial = 0
        for name in arakoon_nodes:
            make_cert(name = name, serial = serial)
            serial = serial + 1

        with lcd(path):
            client = "my_client"
            where("mkdir ./%s" % client)
            make_cert(name = client, serial = serial)
    else:
        acf = arakoon_config_file if acf is None else acf

    arakoon_start_(acf, path, arakoon_nodes, env = env)

def _extend_arakoon_tls(cmd):
    root = TLS['root_dir']
    name = "my_client"
    cmd.extend([
        '-tls-ca-cert', "%s/cacert.pem" % root,
        '-tls-cert',    "%s/%s/%s.pem"     % (root, name, name),
        '-tls-key',     "%s/%s/%s.key"     % (root, name, name)
    ]
    )

def _my_client_tls():
    root = TLS['root_dir']
    cacert = '%s/cacert.pem' % root
    name = "my_client"
    return ('%s/cacert.pem' % root,
            '%s/%s/%s.pem' % (root, name, name),
            '%s/%s/%s.key' % (root, name, name))

def _extend_alba_tls(cmd):
    root = TLS['root_dir']
    name = "my_client"
    cfg = _my_client_tls()
    cmd.extend(['--tls=%s,%s,%s' % cfg])


@task
def arakoon_who_master(arakoon_cfg_file = None,
                       env = env):

    tls = env['alba_tls']
    if arakoon_cfg_file is None:
        if is_true(tls):
            arakoon_cfg_file = TLS['arakoon_config_file']
        else:
            arakoon_cfg_file = arakoon_config_file

    cmd = [
        env['arakoon_bin'],
        '-config', arakoon_cfg_file,
        '--who-master'
    ]
    if is_true(tls):
        _extend_arakoon_tls(cmd)

    where = local
    result = where(' '.join(cmd), capture=True)
    master = result.strip()
    print "master=%s" % result
    return master

@task
def arakoon_remove_dir():
    where = local
    for node in arakoon_nodes:
        where("rm -rf %s/%s" % (ARAKOON_PATH, node))

    where("rm -rf %s/my_client" % ARAKOON_PATH)

@task
def nsm_host_register(cfg_file, albamgr_cfg = arakoon_config_file):
    cmd = [
        env['alba_bin'],
        "add-nsm-host",
        cfg_file,
        "--config",
        albamgr_cfg
    ]
    tls = env['alba_tls']
    if is_true(tls):
        _extend_alba_tls(cmd)

    cmd_line = ' '.join(cmd)
    where = local
    where(cmd_line)

@task
def nsm_host_register_default():
    cfg_file = "%s/cfg/nsm_host_arakoon_cfg.ini" % env['alba_dev']
    albamgr_cfg = arakoon_config_file
    tls = env['alba_tls']
    if is_true(tls):
        cfg_file = TLS['arakoon_config_file']
        albamgr_cfg = TLS['arakoon_config_file']

    nsm_host_register(cfg_file,
                      albamgr_cfg = albamgr_cfg)

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

def _asd_inner(port, path, node_id, slow, multicast,
               env, restart):
    global local_nodeid_prefix
    cfg_path = path + "/cfg.json"
    if not restart:
        limit = 90 if env['osds_on_separate_fs'] else 99
        asd_id = "%i_%i_%s" % (port,node_id, local_nodeid_prefix)
        cfg = {
                'node_id' : "%s_%i" % (local_nodeid_prefix, node_id),
                'home' : path,
                'log_level' : 'debug',
                'asd_id' : asd_id,
                'limit' : limit,
                '__sync_dont_use' : False
        }
        if not multicast:
            cfg ['multicast'] = None

        tls = env['alba_tls']

        if is_true(tls) and restart == False:
            make_cert(name = asd_id )
            path = '%s/%s' % (TLS['root_dir'], asd_id)
            cfg['tls'] = {
                "cert": "%s/%s.pem" % (path, asd_id),
                "key" : "%s/%s.key" % (path, asd_id),
                "port": (port + 500)
            }
        else:
            cfg['port'] = port

        dump_to_cfg_as_json(cfg_path, cfg)

    cmd = [
        env['alba_bin'],
        'asd-start',
        "--config", cfg_path
    ]

    if slow:
        #cmd.append('--slow')
        # mind: we only use fabric for integration tests. This too shall pass
        pass
    return cmd

def _kinetic_inner(port, path):
    cmd =[
        env['kinetic_bin'],
        str(port),
        path,
    ]
    return cmd

def get_osd_port(n):
    tls = env['alba_tls']
    use_tls = is_true(tls)
    return (8500+n if use_tls else 8000+n)

@task
def osd_start(port, path, node_id, kind, slow,
              setup_dir=True, multicast = True,
              env = env, restart = False):
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
                           node_id, slow and port == get_osd_port(0),
                           multicast, env = env, restart = restart)
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
def create_namespace(namespace, abm_cfg = arakoon_config_file):
    cmd = [
        env['alba_bin'],
        "proxy-create-namespace",
        "-h 127.0.0.1 ",
        namespace
    ]

    cmd_line = ' '.join(cmd)
    where = local
    where(cmd_line)

@task
def create_namespace_demo():
    create_namespace(namespace = namespace)

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
                env = env):

    proxy_id = int(proxy_id) # how to enter ints from cli?

    proxy_home = "%s/proxies/%02i" % (ALBA_BASE_PATH, proxy_id)

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

    tls = env['alba_tls']
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
        if is_true(tls):
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
                      n_agents = 1, n_others = 1):
    maintenance_home = "%s/maintenance" % ALBA_BASE_PATH

    local("mkdir -p %s" % maintenance_home)

    maintenance_albmamgr_cfg_file = maintenance_home + "/albamgr.cfg"
    local("cp %s %s" % (abm_cfg, maintenance_albmamgr_cfg_file))
    with lcd(maintenance_home):
        maintenance_cfg = "%s/maintenance.cfg" % maintenance_home
        cfg = { 'albamgr_cfg_file' : maintenance_albmamgr_cfg_file,
                'log_level' : 'debug'
        }
        tls = env['alba_tls']
        if is_true(tls):
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

def wait_for_master(arakoon_cfg_file = arakoon_config_file,
                    max = 15, env = env
    ):
    waiting = True
    count = 0
    m = None
    while waiting:

        try:
            m = arakoon_who_master(arakoon_cfg_file, env = env)

        except:
            m = None

        if m<>None:
            waiting = False
        if count == max:
            print "check if the arakoons are running..."
            local("pgrep -a arakoon")
            raise Exception("this should not take so long")
        count = count + 1
        time.sleep(1)

@task
def claim_local_osds(n, abm_cfg = arakoon_config_file,
                     multicast=True):

    print "claim_local_osds(%i, abm_cfg=%s,multicast=%s)" % (n,abm_cfg, multicast)

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

    def inner(abm_cfg):
        tls = env['alba_tls']
        if is_true(tls):
            abm_cfg = TLS['arakoon_config_file'] #TODO:run_tests_recovery ?

        global claimed_osds
        cmd = [env['alba_bin'],
               'list-available-osds',
               '--config', abm_cfg,
               '--to-json']

        if is_true(tls):
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
                if is_true(tls):
                    _extend_alba_tls(cmd)
                cmd_line = ' '.join(cmd)
                local(cmd_line)
                claimed_osds += 1

    count = 0
    while ((claimed_osds != n) and (count < 60)):
        inner(abm_cfg)
        count += 1
        time.sleep(1)

    assert (claimed_osds == n)

@task
def start_osds(kind, n, slow, multicast=True,
               env = env, restart = False):
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
                  multicast = multicast,
                  env = env,
                  restart = restart
        )

@task
def install_monitoring():
    where = local

    def get_lines(cmd):
        return where(cmd, capture=True).split('\n')

    arakoon_lines = get_lines('pgrep -a arakoon')
    alba_lines    = get_lines('pgrep -a alba')

    monitoring_file = env['monitoring_file']

    with open(monitoring_file,'w')as f:
        for l in arakoon_lines + alba_lines:
            f.write(l)
            f.write('\n')

    def get_pids(lines):
        return [line.split()[0] for line in lines]

    alba_pids = get_pids(alba_lines)
    arakoon_pids = get_pids(arakoon_lines)

    all_pids = arakoon_pids + alba_pids
    cmd = ['pidstat']
    for pid in all_pids:
        cmd.append('-p')
        cmd.append(pid)
    cmd.append('1')
    cmd_line = _detach(cmd, out = monitoring_file)
    where(cmd_line)


@task

def demo_setup(kind = default_kind,
               multicast = True,
               n_agents = 1,
               n_proxies = 1,
               acf = None):

    cmd = [ env['arakoon_bin'], '--version' ]
    local(' '.join(cmd))
    cmd = [ env['alba_bin'], 'version' ]
    local(' '.join(cmd))

    tls = env['alba_tls']
    if acf is None:
        if is_true(tls):
            acf = TLS['arakoon_config_file']
        else:
            acf = arakoon_config_file


    arakoon_start(acf = acf)
    wait_for_master(arakoon_cfg_file = acf)

    if env.get('0.6') :
        cmd = [
        env['alba_bin'],
        'apply-license',
        env['license_file'],
        env['signature'],
        '--config', acf
        ]

    local(' '.join(cmd))
    n_agents = int(n_agents)
    n_proxies = int(n_proxies)

    proxy_start(abm_cfg = acf,
                n_proxies = n_proxies,
                n_others = n_agents)

    maintenance_start(abm_cfg = acf,
                      n_agents = n_agents,
                      n_others = n_proxies)

    nsm_host_register_default()

    start_osds(kind, N, True, multicast)

    claim_local_osds(N, abm_cfg = acf, multicast = multicast)

    create_namespace_demo()
    install_monitoring()

@task
def smoke_test(sudo = 'False'):
    local("pgrep -a alba")
    local("pgrep -a arakoon")

    m = arakoon_who_master()
    print "master:", m
    fuser = "fuser"
    centos = False
    try:
        with open('/etc/os-release','r') as f:
            data = f.read()
            #print data
            if data.find('centos') > 0:
                centos = True
    except:
        pass

    if centos:
        print "we're on centos"
        sudo = True

    my_fuser = "fuser"

    if is_true(sudo):
        my_fuser = "sudo fuser"

    def how_many_osds():
        cmd = "%s -n tcp " % my_fuser
        for i in range(N):
            cmd += " %i" % get_osd_port(i)
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
def deb_integration_test(arakoon_url,
                         alba_url,
                         xml=False):

    local("sudo dpkg -r arakoon | true")
    local("sudo dpkg -r alba | true")
    local("rm -f arakoon_*.deb")
    local("rm -f alba_*.deb")

    local("sudo apt-get -y update")

    local("wget %s" % arakoon_url)
    local("sudo gdebi -n arakoon_*.deb")
    local("wget %s" % alba_url)
    local("sudo gdebi -n alba_*.deb")

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
        local("alba asd-statistics -h 127.0.0.1 -p %i" % get_osd_port(i))

    # TODO: why doesn't this work?
    #with show('debug'):
    #    smoke_test(sudo=True)

    #demo_kill()

    local("sudo dpkg -r arakoon")
    local("sudo dpkg -r alba")

    if xml:
        dump_junit_xml()

@task
def rpm_integration_test(arakoon_url,
                         alba_url,
                         xml=False):

    if not os.path.exists("/etc/redhat-release"):
        raise Exception("should be run on redhat")

    local("sudo yum -y erase arakoon | true")
    local("sudo yum -y erase alba | true")

    local("rm -f arakoon-*")
    local("wget %s" % arakoon_url)
    local("rm -f alba-*")
    local("wget %s" % alba_url)

    local("sudo yum -y update")

    local("sudo yum -y erase arakoon | true")
    local("sudo yum -y erase alba | true")
    local("sudo yum -y --nogpgcheck localinstall arakoon-*")
    local("sudo yum -y --nogpgcheck localinstall alba-*")

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
        local("alba asd-statistics -h 127.0.0.1 -p %i" % get_osd_port(i))
    demo_kill()

    local("sudo yum -y erase arakoon alba")
    if xml:
        dump_junit_xml()
