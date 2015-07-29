#!/usr/bin/python
import subprocess
import os
import time
def shell(cmd):
    r = subprocess.check_output(cmd)
    return r

packaging_dir = './tmp/packaging'

alba_binary = "./ocaml/alba.native"
alba_dependencies = shell(['ldd', alba_binary])

arakoon_binary = '../ARAKOON/arakoon/arakoon.native'
arakoon_dependencies = shell(['ldd', arakoon_binary])

shared_libs_dir = '%s/shared_libs' % packaging_dir
bin_dir = '%s/bin' % packaging_dir

plugin_dir = '%s/plugins' % packaging_dir

def prepare_dirs():
    for d in [shared_libs_dir, bin_dir, plugin_dir]:
        try:
            # remove directory so no artifacts from
            # previous builds can be left behind
            shell(['rm', '-rf', d])
        except subprocess.CalledProcessError:
            pass
        shell(['mkdir','-p' , d])


def copy_shared_libs(dependencies):
    for line in dependencies.split('\n'):
        if line.find('not found') > 0:
            raise Exception(line)
        index = line.find('=>')

        if index >= 0:
            index2 = line.find('(', index)
            if index2 >=0:
                dependency = line[index+3:index2].strip()
                if dependency:
                    simple_name = line[:index].strip()
                    target = '%s/shared_libs/%s' % (packaging_dir, simple_name)
                    cmd = ['cp', dependency, target]
                    shell(cmd)

def copy_executables():
    for e in [alba_binary, arakoon_binary]:
        shell(['cp', e, bin_dir])

def copy_plugins():
    for p in ['albamgr_plugin.cmxs', 'nsm_host_plugin.cmxs']:
        shell(['cp', '%s/%s' % ('./ocaml/', p), plugin_dir])

def generate_script(name, executable):
    fn = '%s/%s' % (bin_dir,name)
    f = open(fn, 'w')
    script_template = """

#!/bin/bash
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
root=${DIR%%/bin}
bin_dir=${root}/bin
shared_libs_dir=${root}/shared_libs
LD_LIBRARY_PATH=$shared_libs_dir $bin_dir/%s $@

"""
    script = script_template % executable
    sscript = script.strip()

    print >> f, sscript
    f.close()
    shell(['chmod', 'u+x', fn])


def generate_archive():
    try:
        version_0 = shell(['git', 'describe', '--dirty'])
    except subprocess.CalledProcessError:
        now = time.strftime("%Y-%m-%d-%Hh%M")
        version_0 = "%s-%s" % (now, shell(['git', 'describe', '--always', '--dirty']))
    version = version_0.strip()
    name="alba-%s" % version
    archive = '%s.tgz' % name

    shell(['tar',
           '-C', packaging_dir,
           '--transform=s/\\./%s/' % name,
           '-zcf', archive,
           '.'
           ])

prepare_dirs()
copy_shared_libs(alba_dependencies)
copy_shared_libs(arakoon_dependencies)
copy_executables()
copy_plugins()

generate_script('alba_cli',   'alba.native')
generate_script('arakoon', 'arakoon.native')
generate_archive()
