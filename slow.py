import time
import datetime
import sys
if len(sys.argv) < 2:
    print "needs a filename as argument"
    sys.exit(1)

fn = sys.argv[1]


def voldrv_timestamp(line):
    ls = line.strip()
    timestamp_s = ls[:26]
    try:
        timestamp = time.mktime(datetime.datetime.strptime(timestamp_s,
                                                           "%Y-%m-%d %H:%M:%S:%f").timetuple())
        return timestamp
    except:
        return None

def alba_timestamp(line):
    # Apr  1 13:15:25.6128
    ls = line.strip()
    timestamp_s = ls[:20]
    #print timestamp_s
    month_s = timestamp_s[ :3]
    day_s   = timestamp_s[4:7]
    hour_s  = timestamp_s[7:9]
    min_s   = timestamp_s[10:12]
    sec_s   = timestamp_s[13:15]
    frac_s  = timestamp_s[16:]
    #print month_s, day_s, hour_s,min_s,sec_s,frac_s
    month = 4
    day = int(day_s)
    hour = int(hour_s)
    minute = int(min_s)
    second = int(sec_s)
    microsecond = int(frac_s) / 10
    dt = datetime.datetime(2015,
                           month,
                           day,
                           hour,
                           minute,
                           second,
                           0,
                           None)
    timestamp = time.mktime(dt.timetuple())
    return timestamp

with open(fn,'r') as f:
    line = f.readline()
    pt = None
    while line:
        #timestamp = voldrv_timestamp(line)
        timestamp = alba_timestamp(line)
        #print timestamp
        if pt :
            if timestamp:
                diff = timestamp - pt
                #print diff
                if diff > 2.0 :
                    print diff, line.strip()
        pt = timestamp
        line = f.readline()
