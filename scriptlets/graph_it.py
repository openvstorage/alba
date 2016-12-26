import argparse

'''
   process contents of pidstat output
   (fe the monitor.txt file created by 'setup')
   and produce a graph

'''


def extract_process_names(lines):
    p = {}
    for line in lines:
        parts = line.split()
        print parts
        pid = parts[0]
        if parts[1].find('arakoon') > 0:
            process_name = parts[3]
            p[pid] = process_name
        elif parts[2].find('proxy-start') >= 0:
            process_name = 'proxy'
            p[pid] = process_name
        elif parts[2].find('maintenance') >= 0:
            process_name = 'maintenance'
            p[pid] = process_name
        elif parts[2].find('asd-start') >= 0:
            process_name = 'asd' + parts[4][-11:-9]
            print process_name
            p[pid] = process_name
    return p

def main(monitor_file):
    process_lines = None
    with open(monitor_file, 'r') as f:
        d = f.readlines()
        c = 0
        for line in d:
            if line.find('Linux') <0:
                c = c +1
            else:
                break
        process_lines = d[:c]
        d = d[c+2:]

    pid2name = extract_process_names(process_lines)
    name2pid = {}
    for pid,name in pid2name.items():
        name2pid[name] = pid

    print 'pid2name', pid2name
    print 'name2pid', name2pid
    r = {}
    c = 0
    pid_set = {}
    n = 17
    header = d[0].split()
    header = header [1:] # compensate for the '#'
    pid_column = header.index('PID')
    cpu_column = header.index('%CPU')
    kBw_column = header.index('kB_wr/s')
    kBr_column = header.index('kB_rd/s')

    print "pid_column", pid_column
    print "cpu_column", cpu_column
    print "kBw_column", kBw_column
    print "kBr_column", kBr_column

    t0 = float(20000000000)

    class Sample:
        def __init__(self,cpu,r,w):
            self.cpu = cpu
            self.r = r
            self.w = w

    for l in d:
        if l.find('UID') > 0:
            continue
        s = l.strip()
        parts = s.split()

        if len(parts) == 0:
            continue

        t = float(parts[0])
        t0 = min(float(t),t0)
        t = t - t0
        pid = parts[pid_column]

        #print l
        #print header

        #print parts
        #print "\tpid=",pid
        #print "\n"

        pid_set[pid] = True
        pid_cpu = parts[cpu_column]
        pid_r = parts[kBr_column]
        pid_w = parts[kBw_column]
        t_line = r.get(t)
        if t_line is None:
            t_line = {}
            r[t] = t_line

        t_line[pid] = Sample(pid_cpu,pid_r,pid_w)
        #print t_line
        c = c + 1
        #if c == 20:
        #    break
    #print "t0=",t0

    pids = pid2name.keys()
    t_lines = r.keys()
    t_lines.sort()
    header = "#        "
    col = 1
    for pid,name in pid2name.items():
        header = header + ('%25s' % ("(%i)%s/%s:cpu" % (col,pid,name) ))
        col = col + 1
    for pid,name in pid2name.items():
        header = header + ('%25s' % ("(%i)%s/%s:kBw" % (col,pid,name) ))
        col = col + 1

    with open('processed.csv','w') as f:
        print >>f, header
        for t_line in t_lines:
            row_data = r[t_line]
            line = "%s;" % t_line
            for pid in pids:
                sample = row_data.get(pid)
                cpu = '0.0' if sample is None else sample.cpu
                line = line + "%24s;" % cpu
            for pid in pids:
                sample = row_data.get(pid)
                kbw = '0.0' if sample is None else sample.w
                line = line + "%24s;" % kbw
            print >>f, line

    t0 = t_lines[0]
    t1 = t_lines[-1]

    with open('processed.gnuplot','w') as f:
        print >>f, 'set datafile separator ";"'
        print >>f, 'set style data lines'
        print >>f, 'set term svg'
        print >>f, "set key outside" # TODO only legend those that are interesting
        print >>f, "set output 'processed.svg'"
        print >>f, 'set multiplot layout 2,1'
        #print >>f, 'set xdata time'
        #print >>f, 'set xtics rotate by 45 right'

        #print >>f, 'set timefmt "%H:%M:%S"'
        #print >>f, 'set format x "%H:%M:%S"'
        print >>f, 'set xrange ["%s" : "%s"]' % (t0,t1)
        #print >>f, 'set yrange [ 0: 150]'
        print >>f, 'set grid'
        print >>f, 'set ylabel "%CPU"'
        print >>f, 'set xlabel "seconds"'



        col = 2
        for pid in pids:
           name = pid2name[pid]
           pre = ''
           if col == 2:
               pre = "plot "
           post = ",\\"
           if pid == pids[-1]:
               post = ""
           # style = "smooth bezier"
           style = ""
           print >>f, "%s 'processed.csv' using 1:%i %s t '%s' %s" % (pre, col, style, name, post)
           col = col + 1
        print >>f, ""
        print >>f, 'set ylabel "kBw"'
        #print >>f, 'set yrange [0:20000]'

        col = 2
        for pid in pids:
           name = pid2name[pid]
           pre = ''
           if col == 2:
               pre = "plot "
           post = ",\\"
           if pid == pids[:-1]:
               post = ""
           # style = "smooth bezier"
           style = ""
           real_col = len(pids) + col -1
           print >>f, "%s 'processed.csv' using 1:%i %s t '%s' %s" % (pre, real_col, style, name, post)
           col = col + 1

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--monitor_file",
                        required = True,
                        help = "(demo_setup monitoring)")
    options = parser.parse_args()
    monitor_file = options.monitor_file
    main(monitor_file)
