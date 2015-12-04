"""
Copyright 2015 iNuron NV

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

from env import *
import alba
from fabric.api import local, task, warn_only
from fabric.context_managers import shell_env
import time
from uuid import uuid4
import sha
import json
import hashlib
import sys

@task
def check():
    where = local
    where("test -f %s" % env['alba_bin'])
    where("test -f %s" % env['arakoon_bin'])

@task
def compile():
    where = local
    where("cd cpp/ && tup")
    where("make")

@task
def clean():
    where = local
    with warn_only():
        where("rm -f cpp/bin/*.out")
        where("rm -f cpp/src/caas/*.o")
        where("rm -f cpp/src/examples/*.o")
        where("rm -f cpp/src/lib/*.o")
        where("rm -f cpp/src/tests/*.o")
        where("cd ocaml/ && rm -rf _build")


def setup_demo_alba(kind = default_kind):
    alba.demo_kill()
    alba.demo_setup()

@task
def run_tests_cpp(xml=False, kind=default_kind,
                  valgrind = False,
                  filter = None):

    setup_demo_alba(kind)

    where = local
    where("rm -rf %s/ocaml/" % ALBA_BASE_PATH)
    cmd = "LD_LIBRARY_PATH=./cpp/lib ./cpp/bin/unit_tests.out"

    if xml:
        cmd = cmd + " --gtest_output=xml:gtestresults.xml"
    if valgrind:
        cmd = "valgrind " + cmd
    if filter:
        cmd = cmd + " --gtest_filter=%s" % filter
    where(cmd)

@task
def run_tests_ocaml(xml=False,
                    kind = default_kind,
                    dump = None,
                    filter = None):
    alba.demo_kill()
    alba.arakoon_start()
    alba.wait_for_master()

    alba.maintenance_start()
    alba.proxy_start()
    alba.nsm_host_register_default()

    alba.start_osds(kind, N, False)

    tls = env['alba_tls']
    use_tls = is_true(tls)
    if use_tls:
        # make cert for extra asd (test_discover_claimed)
        alba.make_cert(name = 'test_discover_claimed')

    alba.claim_local_osds(N, abm_cfg = arakoon_config_file)

    where = local
    where("rm -rf %s/ocaml/" % ALBA_BASE_PATH)

    cmd = [env['alba_bin'], "unit-tests"]
    if xml:
        cmd.append(" --xml=true")
    if filter:
        cmd.append(" --only-test=%s" % filter)

    if use_tls:
        alba._extend_alba_tls(cmd)

    print cmd
    cmd_line = ' '.join(cmd)
    if dump:
        cmd_line += " > %s" % dump

    where(cmd_line)


def timed_test(name, test):
    t0 = time.time()
    r = True
    try:
        test ()
    except:
        e = sys.exc_info()[0]
        print e
        r = False

    t1 = time.time ()
    delta = t1 - t0
    return (name, r, delta)


@task
def run_tests_cli():
    alba.demo_kill()
    alba.demo_setup()
    tls = env['alba_tls']
    port = '8501' if is_true(tls) else '8001'
    host = '::1'
    where = local

    def _asd( what, extra, tls = tls):
        cmd = [env['alba_bin'],
               what,
               '-h', host,
               '-p', port]
        cmd.extend(extra)
        if is_true(tls):
            alba._extend_alba_tls(cmd)
        return cmd

    def _run(cmd):
        cmd_line = ' '.join(cmd)
        result = where(cmd_line, capture = True)
        return result

    def test_asd_get_version():
        cmd = _asd('asd-get-version', [] )
        result = _run(cmd)
        t = eval(result)
        print t
        assert (len(t) == 4)
        print "ok"

    def test_asd_get_statistics():
        cmd = _asd('asd-statistics',['--to-json'])
        result = _run(cmd)
        t = json.loads(result)

    def test_asd_crud():
        k = 'the_key'
        v = 'the_value'
        asd_set = _asd('asd-set', [k,v])
        _run(asd_set)

        asd_get = _asd('asd-multi-get',[k])
        v_s = _run(asd_get)
        assert (v_s.find(v) > 0)

        asd_delete = _asd('asd-delete',[k])
        _run(asd_delete)
        v_s2 = _run(asd_get)
        print v_s2
        assert (v_s2 == '[None]')

    def test_asd_cli_env():
        print "test_asd_cli_env"
        if is_true(env['alba_tls']):
            cmd = _asd('asd-get-version', [], tls = False)
            x = alba._my_client_tls()
            with shell_env(ALBA_CLI_TLS='%s,%s,%s' % x):
                cmd_line = ' ' . join(cmd)
                result = where(cmd_line, capture=True)
                print result

    def create_example_preset():
        cmd = [ env['alba_bin'],
                'create-preset', 'example',
                '--config', './cfg/albamgr_example_arakoon_cfg.ini',
                '< cfg/preset.json'
        ]
        tls = env['arakoon_tls']

        if is_true(tls):
            _extend_alba_tls(cmd)

        cmd_line = ' '.join(cmd)
        local (cmd_line)

    suite_name = "run_tests_cli"
    results = []
    tests = [
        ("asd_get_version", test_asd_get_version),
        ("asd_cli_env", test_asd_cli_env),
        ("asd_get_statistics", test_asd_get_statistics),
        ("asd_crud", test_asd_crud)
    ]

    for test_name, test  in tests:
        r = timed_test(test_name,test)
        results.append(r)
    return "cli", results


    test_asd_crud()


@task
def run_tests_voldrv_backend(xml=False, kind = default_kind,
                             filter = None, dump = None):
    alba.demo_kill()
    alba.demo_setup(kind)
    time.sleep(1)
    where = local
    cmd0 = ""
    #cmd0 = "valgrind -v --track-origins=yes --leak-check=full --leak-resolution=high "
    cmd = cmd0 + "%s --skip-backend-setup 1 --backend-config-file ./cfg/backend.json" % env['voldrv_backend_test']
    if filter:
        cmd = cmd + " --gtest_filter=%s" % filter
    if xml:
        cmd = cmd + " --gtest_output=xml:gtestresults.xml"
    if dump:
        cmd = cmd + " > %s 2>&1" % dump
    where(cmd)

@task
def run_tests_voldrv_tests(xml=False, kind = default_kind, dump = None):
    alba.demo_kill()
    alba.demo_setup(kind)
    time.sleep(1)
    where = local
    cmd = "%s --skip-backend-setup 1 --backend-config-file ./cfg/backend.json --gtest_filter=SimpleVolumeTests/SimpleVolumeTest* --loglevel=error" % env['voldrv_tests']
    if xml:
        cmd = cmd + " --gtest_output=xml:gtestresults.xml"
    if dump:
        cmd = cmd + " 2> %s" % dump # in this case, we need sterr
    where(cmd)

@task
def run_tests_disk_failures(xml=False):
    alba.demo_kill()
    alba.demo_setup()
    time.sleep(1)
    where = local
    cmd = [env['failure_tester']]
    if xml:
        cmd.append(" --xml=true")

    tls = env['alba_tls']
    if is_true(tls):
        alba._extend_alba_tls(cmd)

    cmd_s = " ".join(cmd)
    where(cmd_s)

@task
def run_tests_stress(kind = default_kind, xml = False):
    alba.demo_kill()
    alba.demo_setup(kind = kind)
    time.sleep(1)
    where = local
    #
    n = 3000
    def build_cmd(command):
        cmd = [
        env['alba_bin'],
            command
        ]
        return cmd

    def create_namespace_cmd(name):
        cmd = build_cmd('proxy-create-namespace')
        cmd.append(name)
        cmd_s = ' '.join(cmd)
        return cmd_s

    def list_namespaces_cmd():
        cmd = build_cmd('list-namespaces')
        tls = env['alba_tls']
        if is_true(tls):
            alba._extend_alba_tls(cmd)

        cmd.append('--to-json')
        cmd.append('2> /dev/null')
        cmd_s = ' '.join(cmd)
        return cmd_s

    template = "%20i"

    for i in xrange(n):
        name = template % i
        cmd_s = create_namespace_cmd(name)
        where (cmd_s)
    # they exist?
    v = where(list_namespaces_cmd(),
              capture = True)
    parsed = json.loads(v)
    print parsed
    assert parsed["success"] == True
    ns = parsed["result"]
    print len(ns)
    assert (len(ns) == n+1)

    if xml:
        alba.dump_junit_xml()

@task
def run_tests_recovery(xml = False, tls = 'False'):
    alba.demo_kill()

    # set up separate albamgr & nsm host
    abm_cfg = "cfg/test_abm.ini"
    nsm_cfg = "cfg/test_nsm_1.ini"
    alba.arakoon_start_(abm_cfg, "%s/abm_0" % ALBA_BASE_PATH, ["micky_0"])
    alba.arakoon_start_(nsm_cfg, "%s/nsm_1" % ALBA_BASE_PATH, ["zicky_0"])

    alba.wait_for_master(abm_cfg)
    alba.wait_for_master(nsm_cfg)

    alba.nsm_host_register(nsm_cfg, albamgr_cfg = abm_cfg)

    alba.maintenance_start(abm_cfg = abm_cfg)
    alba.proxy_start(abm_cfg = abm_cfg)

    N = 3
    alba.start_osds("ASD", N, False)

    alba.claim_local_osds(N, abm_cfg = abm_cfg)

    alba.maintenance_stop()

    ns = 'test'
    alba.create_namespace(ns, abm_cfg = abm_cfg)

    obj_name = 'alba_binary'
    cmd = [
        env['alba_bin'],
        'upload-object', ns, env['alba_bin'], obj_name,
        '--config', abm_cfg
    ]
    local(' '.join(cmd))

    cmd = [
        env['alba_bin'],
        'show-object', ns, obj_name,
        '--config', abm_cfg
    ]
    local(' '.join(cmd))

    checksum1 = local('md5sum %s' % env['alba_bin'], capture=True)

    # kill nsm host
    local('fuser -n tcp 4001 -k')
    local('rm -rf %s/nsm_1' % ALBA_BASE_PATH)

    cmd = [
        env['alba_bin'],
        'update-nsm-host',
        nsm_cfg,
        '--lost',
        '--config', abm_cfg
    ]
    local(' '.join(cmd))

    # start new nsm host + register it to albamgr
    alba.arakoon_start_("cfg/test_nsm_2.ini", "%s/nsm_2" % ALBA_BASE_PATH, ["ticky_0"])
    alba.wait_for_master("cfg/test_nsm_2.ini")
    cmd = [
        env['alba_bin'],
        "add-nsm-host",
        'cfg/test_nsm_2.ini',
        "--config",
        abm_cfg
    ]
    local(' '.join(cmd))

    # recover namespace to the new nsm host
    cmd = [
        env['alba_bin'],
        'recover-namespace', ns, 'ticky',
        '--config', abm_cfg
    ]
    local(' '.join(cmd))

    cmd = [
        env['alba_bin'],
        'deliver-messages',
        '--config', abm_cfg
    ]
    local(' '.join(cmd))
    local(' '.join(cmd))

    # bring down one of the osds
    # we should be able to handle this...
    alba.osd_stop(8000)

    local('mkdir -p %s/recovery_agent' % ALBA_BASE_PATH)
    cmd = [
        env['alba_bin'],
        'namespace-recovery-agent',
        ns, '%s/recovery_agent' % ALBA_BASE_PATH, '1', '0',
        "--config", abm_cfg,
        "--osd-id 1 --osd-id 2"
    ]
    local(' '.join(cmd))

    dfile = 'destination_file.out'
    local('rm -f %s' % dfile)
    cmd = [
        env['alba_bin'],
        'download-object', ns, obj_name, dfile,
        "--config", abm_cfg
    ]
    local(' '.join(cmd))

    checksum1 = hashlib.md5(open(env['alba_bin'], 'rb').read()).hexdigest()
    checksum2 = hashlib.md5(open(dfile, 'rb').read()).hexdigest()

    print "Got checksums %s & %s" % (checksum1, checksum2)

    assert (checksum1 == checksum2)

    # TODO 1
    # iets asserten ivm hoeveel fragments er in manifest aanwezig zijn
    # dan osd 0 starten en recovery opnieuw draaien
    # die moet kunnen de extra fragments goed benutten

    # TODO 2
    # object eerst es overschrijven, dan recovery doen
    # en zien of we laatste versie krijgen

    # TODO 3
    # add more objects (and verify them all)

    # TODO 4
    # start met 1 osd die alive is

    if xml:
        alba.dump_junit_xml()


@task
def spreadsheet_my_ass(start=0, end = 13400):
    env['osds_on_separate_fs'] = True
    start = int(start)
    def fresh():
        alba.demo_kill()
        alba.demo_setup()

        # deliver osd & nsm messages
        time.sleep(1)
        cmd = [
            env['alba_bin'],
            'deliver-messages',
            '--config', arakoon_config_file
        ]
        local(' '.join(cmd))

        # stop asd 0
        alba.osd_stop(8000)

    if start == 0:
        fresh()

    # fill the demo namespace
    local("dd if=/dev/urandom of=%s/obj bs=40K count=1" % ALBA_BASE_PATH)
    for i in xrange(start, end):
        cmd = [
            env['alba_bin'],
            'proxy-upload-object',
            'demo', '%s/obj' % ALBA_BASE_PATH,
            str(i)
        ]
        local(" ".join(cmd))

    # show the disk usage
    local("df")

    # restart asd 0 as a not slow asd
    alba._detach(
        [ env['alba_bin'], 'asd-start', '--config', '%s/asd/00/cfg.json' % ALBA_BASE_PATH ],
        out='%s/asd/00/output' % ALBA_BASE_PATH
    )

    # let maintenance process do some rebalancing
    time.sleep(120)
    local("df")


@task
def run_test_asd_start(xml=False):
    alba.demo_kill()
    alba.demo_setup()

    object_location = "%s/obj" % ALBA_BASE_PATH
    local("dd if=/dev/urandom of=%s bs=1M count=1" % object_location)

    for i in xrange(0, 1000):
        cmd = [
            env['alba_bin'],
            'proxy-upload-object',
            'demo', object_location,
            str(i)
        ]
        local(" ".join(cmd))

    for i in xrange(0, N):
        alba.osd_stop(8000 + i)

    alba.start_osds(default_kind, N, True, restart = True)
    time.sleep(1)

    cmd = [
        env['alba_bin'],
        'proxy-upload-object',
        'demo', object_location,
        "fdskii", "--allow-overwrite"
    ]
    with warn_only():
        # this clears the borked connections from the
        # asd connection pools
        local(" ".join(cmd))
        local(" ".join(cmd))
        local(" ".join(cmd))

    r = local(" ".join(cmd))
    print r

    local(" ".join([
        env['alba_bin'],
        'proxy-download-object',
        'demo', '1', '%s/obj2' % ALBA_BASE_PATH
    ]))

    alba.smoke_test()

    if xml:
        alba.dump_junit_xml()

@task
def run_test_big_object():
    def _inner():
        alba.demo_kill()
        alba.demo_setup()
        cmd = [
            env['alba_bin'],
            'create-preset', 'preset_no_compression',
            '--config', arakoon_config_file,

        ]
        tls = env['alba_tls']
        if is_true(tls):
            alba._extend_alba_tls(cmd)

        cmd.append('< ./cfg/preset_no_compression.json')
        # create the preset in albamgr
        local(" ".join(cmd))

        cmd = [
            env['alba_bin'],
            'create-namespace', 'big', 'preset_no_compression',
            '--config', arakoon_config_file
        ]
        if is_true(tls):
            alba._extend_alba_tls(cmd)
        # create new namespace with this preset
        local(" ".join(cmd))

        object_file = "%s/obj" % ALBA_BASE_PATH
        # upload a big object
        local("truncate -s 2G %s" % object_file)
        local(" ".join([
            'time', env['alba_bin'],
            'proxy-upload-object', 'big', object_file, 'bigobj'
        ]))
        # note this may fail with NoSatisfiablePolicy from time to time

        local(" ".join([
            'time', env['alba_bin'],
            'proxy-download-object', 'big', 'bigobj', '%s/obj_download' % ALBA_BASE_PATH
        ]))
        local("rm %s/obj_download" % ALBA_BASE_PATH)

        # policy says we can lose a node,
        # so stop and decommission osd 0 to 3
        for i in range(0,4):
            port = 8000+i
            alba.osd_stop(port)
            long_id = "%i_%i_%s" % (port, 2000, alba.local_nodeid_prefix)
            cmd = [
                env['alba_bin'],
                'decommission-osd', '--long-id', long_id,
                '--config', arakoon_config_file
            ]
            if is_true(tls):
                alba._extend_alba_tls(cmd)
            local(" ".join(cmd))

        # TODO
        # wait for maintenance process to repair it
        # (give maintenance process a kick?)
        # report mem usage of proxy & maintenance process

    test_name = "big_object"
    result = timed_test(test_name, _inner)
    suite_name = test_name
    return suite_name, [result]

@task
def run_tests_compat(xml = True):
    def test(old_proxy, old_plugins, old_asd):
        try:
            alba.smoke_test()
            def alba_cli(extra, old = False, capture = False):
                key = 'alba.0.6' if old else 'alba_bin'
                binary = env[key]
                cmd = [binary]
                cmd.extend(extra)
                cmd_line = ' '.join(cmd)
                r = local(cmd_line, capture = capture)
                return r

            obj_name = 'alba_binary'
            ns = 'demo'
            cfg = './cfg/albamgr_example_arakoon_cfg.ini'
            alba_cli(['proxy-upload-object', ns , env['alba_bin'], obj_name])
            alba_cli(['proxy-download-object', ns, obj_name, '/tmp/downloaded.bin'])
            alba_cli(['delete-object', ns, obj_name, '--config', cfg ])

            # some explicit backward compatible operations
            r = alba_cli (['list-all-osds', '--config', cfg, '--to-json'], old = True, capture = True)
            osds = json.loads(r)
            assert osds['success'] == True
            long_id = osds['result'][0]['long_id']

            # decommission 1 asd:
            alba_cli (['decommission-osd', '--long-id', long_id,
                           '--config', cfg], old = True, capture = True)

            print "now, list them"

            r = alba_cli (['list-decommissioning-osds', '--config', cfg, '--to-json'],
                          old = True,
                          capture = True)
            decommissioning_osds = json.loads(r)
            print decommissioning_osds['success']
            assert decommissioning_osds['success'] == True


        except Exception, e:
            print e
            with warn_only():
                local ("which alba.0.6")
                local ("pgrep -a alba")
                local ("pgrep -a arakoon")
                local ("which fuser")
                local ("sudo fuser -n tcp 10000 8001")

            raise

    def deploy_and_test(old_proxy, old_plugins, old_asds):
        tls = 'False'

        alba.demo_kill()

        env_old = env.copy()
        old_alba_home = './bin/0.6'
        env_old['alba_bin'] = env.get('alba.0.6')
        env_old['alba_plugin_path'] = env.get('ALBA_06_PLUGIN_PATH', '/usr/lib/alba')
        env_old['license_file'] = '%s/community_license' % old_alba_home
        env_old['signature']    = '''3cd787f7a0bcb6c8dbf40a8b4a3a5f350fa87d1bff5b33f5d099ab850e44aaeca6e3206b595d7cb361eed28c5dd3c0f3b95531d931a31a058f3c054b04917797b7363457f7a156b5f36c9bf3e1a43b46e5c1e9ca3025c695ef366be6c36a1fc28f5648256a82ca392833a3050e1808e21ef3838d0c027cf6edaafedc8cfe2f2fc37bd95102b92e7de28042acc65b8b6af4cfb3a11dadce215986da3743f1be275200860d24446865c50cdae2ebe2d77c86f6d8b3907b20725cdb7489e0a1ba7e306c90ff0189c5299194598c44a537b0a460c2bf2569ab9bb99c72f6415a2f98c614d196d0538c8c19ef956d42094658dba8d59cfc4a024c18c1c677eb59299425ac2c225a559756dee125ef93c38c211cda69c892d26ca33b7bd2ca95f15bbc1bb755c46574432005b8afcab48a0a5ed489854cec24207cddc7ab632d8715c1fb4b1309b45376a49e4c2b4819f27d9d6c8170c59422a0b778b9c3ac18e677bc6fa6e2a2527365aca5d16d4bc6e22007debef1989d08adc9523be0a5d50309ef9393eace644260345bb3d442004c70097fffd29fe315127f6d19edd4f0f46ae2f10df4f162318c4174b1339286f8c07d5febdf24dc049a875347f6b2860ba3a71b82aba829f890192511d6eddaacb0c8be890799fb5cb353bce7366e8047c9a66b8ee07bf78af40b09b4b278d8af2a9333959213df6101c85dda61f2944237c8'''
        env_old['arakoon_bin'] = env.get('ARAKOON_189_BIN', '/usr/bin/arakoon')
        if old_plugins:
            arakoon_env = env_old
        else:
            arakoon_env = env

        alba.demo_kill(env = env_old)

        alba.arakoon_start(env = arakoon_env)
        alba.wait_for_master(env = arakoon_env)

        if old_plugins:
            cmd = [
                env_old['alba_bin'],
                'apply-license',
                env_old['license_file'],
                env_old['signature'],
                '--config', arakoon_config_file
            ]
            cmd_line = ' '.join(cmd)
            local(cmd_line)

        alba.maintenance_start()

        proxy_env = env_old if old_proxy else env

        alba.proxy_start(env = proxy_env)


        alba.nsm_host_register_default()


        if old_asds:
            asd_env = env_old
        else:
            asd_env = env

        kind = default_kind
        alba.start_osds(kind, N, False, env = asd_env)
        alba.claim_local_osds(N, abm_cfg = arakoon_config_file)
        alba.create_namespace_demo()
        #
        test(old_proxy, old_plugins, old_asds)


    results = []
    flavours = range(8)

    t0 = time.time()
    for flavour in flavours:
        old_proxy   = flavour & 4 == 4
        old_plugins = flavour & 2 == 2
        old_asds    = flavour & 1 == 1
        t0_test = time.time()

        result = False
        try:
            deploy_and_test(old_proxy,old_plugins,old_asds)
            result = True
        except:
            pass

        t1_test = time.time()
        delta_test = t1_test - t0_test
        test_name = "flavour_%i" % flavour
        results.append((test_name, result, delta_test))

    failures = filter(lambda x: not x[1], results)
    t1 = time.time()
    delta = t1 - t0

    if is_true(xml):
        from junit_xml import TestSuite, TestCase
        test_cases = []
        for (name,result, delta) in results:
            test_case = TestCase(name, 'TestCompat', elapsed_sec = delta)
            if not result:
                test_case.add_error_info(message = "failed")
            test_cases.append(test_case)

        ts = [TestSuite("compatibility", test_cases)]
        with open('./testresults.xml', 'w') as f:
            TestSuite.to_file(f,ts)
    else:
        print results

@task
def run_test_arakoon_changes ():
    def _inner():
        alba.demo_kill ()
        alba.demo_setup(acf = arakoon_config_file_2)
        # 2 node cluster, and arakoon_0 will be master.
        def stop_node(node_name):
            r = local("pgrep -a arakoon | grep '%s'" % node_name, capture = True)
            info = r.split()
            pid = info[0]
            local("kill %s" % pid)

        def start_node(node_name, cfg):
            inner = [
                env['arakoon_bin'],
                "--node", node_name,
                "-config", cfg
            ]
            cmd_line = alba._detach(inner)
            local(cmd_line)

        def signal_alba(process_name,signal):
            r = local("pgrep -a alba | grep %s" % process_name, capture = True)
            info = r.split()
            pid = info[0]
            local("kill -s %s %s" % (signal, pid))

        def wait_for(delay):
            n = int(delay)
            print "sleeping %i" % n
            while n:
                print "\t%i" % n
                time.sleep(1)
                n = n - 1

        # 2 node cluster, and arakoon_0 is master.
        wait_for(10)
        stop_node('arakoon_0')
        stop_node('witness_0')
        # restart them with other config

        start_node('arakoon_1', arakoon_config_file)
        start_node('witness_0', arakoon_config_file)

        wait_for(20)
        start_node('arakoon_0', arakoon_config_file)

        maintenance_home = "%s/maintenance" % ALBA_BASE_PATH
        maintenance_cfg = maintenance_home + "/albamgr.cfg"
        local("cp %s %s" % (arakoon_config_file, maintenance_cfg))
        signal_alba('maintenance','USR1')
        wait_for(120)
        cfg_s = local("%s proxy-client-cfg | grep port | wc" % env['alba_bin'], capture=True)
        c = cfg_s.split()[0]
        assert (c == '3') # 3 nodes in config

    test_name = "arakoon_changes"
    result = timed_test(test_name, _inner)
    suite_name = test_name
    return suite_name, [result]



@task
def run_everything_else(xml = False):
    mega_suite = []
    tests = [
        run_test_arakoon_changes,
        run_tests_cli,
        run_test_big_object
    ]
    for x in tests:
        r = x ()
        mega_suite.append(r)

    print mega_suite
    if is_true(xml):
        from junit_xml import TestSuite, TestCase
        test_cases = []
        for (suite, results) in mega_suite:
            for (name,result, delta) in results:
                test_case = TestCase(name, suite, elapsed_sec = delta)
                if not result:
                    test_case.add_error_info(message = "failed")
                test_cases.append(test_case)

        ts = [TestSuite("run_everything_else", test_cases)]
        with open('./testresults.xml', 'w') as f:
            TestSuite.to_file(f,ts)
    else:
        print mega_suite
