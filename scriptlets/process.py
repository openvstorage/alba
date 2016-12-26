import json
from operator import itemgetter

"""
    see which ASDs performed a partial read (and how many)
    first do a
    alba asd-multistats --all > all_asd_stats.json
    then run this

    edit ad lib
"""

fn = './all_asd_stats.json'

with open(fn, 'r') as f :
    j = json.load(f)
    result = j['result']
    partial_gets = []
    failures = []

    for asd_id in result.keys():
        asd_result = result[asd_id]
        if asd_result['success'] :
            inner_result = asd_result['result']
            if inner_result.has_key('PartialGet'):
                partial_get = inner_result['PartialGet']
                n   = partial_get['n']
                _min = partial_get['min']
                _avg = partial_get['avg']
                _max = partial_get['max']
                v = (asd_id, n, _min,_avg, _max)
                partial_gets.append(v)
            else:
                v = (asd_id, "no partial gets")
                failures.append(v)

        else:
            v = asd_id, asd_result['result']
            failures.append(v)


    partial_gets.sort(key = itemgetter(4))
    for p in partial_gets:
        print "{: ^32s} ; {: <8d} ; {: <8f} ; {: <8f} ; {: <8f}".format(*p)
    print
    for p in failures:
        print p[0], p[1]

    gmin = 1000.0
    gmax = 0.0
    sum_avg = 0.0
    n = 0
    for p in partial_gets:
        _min = p[2]
        _avg = p[3]
        _max = p[4]
        if _min < gmin:
            gmin = _min
        if _max > gmax:
            gmax = _max
        sum_avg = sum_avg + _avg
        n = n + 1

    gavg = sum_avg / float (n)
    print "global:"
    print gmin,gavg, gmax
