import subprocess

"""
   iterate over the objects of a namespace
   and see which OSDs are used. Was hacked up to inspect a distribution bug
   you will need to modify the script:
   fill in namespace & alba_config
"""

namespace = "pread_bench"
alba_bin = "alba"
alba_config = "arakoon://config/ovs/vpools/85c31654-9431-42e7-8f6a-02240a3522ca/proxies/67602a9d-9cee-45c7-97ba-09344e5a604f/config/abm?ini=%2Fopt%2FOpenvStorage%2Fconfig%2Farakoon_cacc.ini"
#alba_bin = "./ocaml/alba.native"
#alba_config = "tmp/alba_hdd/tmp/arakoon/abm.ini"

def get_output():
    output = subprocess.check_output([alba_bin,
                                      "list-objects",
                                      namespace,
                                      "--config", alba_config])
    return output

def get_object_names(output):
    results = []
    for line in output.split("\n"):
        start = line.find('"')
        end = line.rfind('"')
        v0 = line[start : end+1].split(";")
        #print "v0=", v0
        v1 = filter((lambda x : len(x) > 1), v0)
        #print "v1=", v1
        for x in v1:
            r = x.strip()[1:-1]
            results.append(r)

    return results




def osds_of_object(object_name):
    output = subprocess.check_output([alba_bin,
                                      "show-object",namespace,
                                      object_name,
                                      "--config", alba_config
    ])
    lines = output.split('\n')
    count = 0
    for line in lines:
        #print line
        if line.find('fragment_locations') > 0:
            start = count+1
        if line.find('fragment_checksum') >0:
            end = count
        count = count +1


    location_lines = lines[start:end]
    #   [[((Some 4L), 0); ((Some 10L), 0); ((Some 2L), 0); ((Some 5L), 0);
    osd_ids = []
    for loc_line in location_lines:
        entries = loc_line.split(';')
        for entry in entries:
            start = entry.find('Some ')
            if start > 0 :
                end = entry.find('L', start+1)
                osd_id_s =  entry[start+5:end]
                osd_id = int(osd_id_s)
                osd_ids.append(osd_id)

    return osd_ids

output = get_output()
object_names = get_object_names(output)
r = {}
firsts = {}
for object_name in object_names:
    osds = osds_of_object(object_name)
    print object_name, osds
    for osd in osds:
        count = r.get(osd, 0) +1
        r[osd] = count

    first_osd = osds[0]
    count = firsts.get(first_osd, 0) + 1
    firsts[first_osd] = count
    print "all", r,len(r.keys())
    print "firsts", firsts, len(firsts.keys())
