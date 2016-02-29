/* nicked from baardskeerder (LGPL) and adapted */

#define _XOPEN_SOURCE 600
#define _FILE_OFFSET_BITS 64
#define _GNU_SOURCE

#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/ioctl.h>

#include <linux/falloc.h>
#include <linux/fs.h>
#include <linux/fiemap.h>

#include <caml/mlvalues.h>
#include <caml/alloc.h>
#include <caml/memory.h>
#include <caml/fail.h>
#include <caml/unixsupport.h>
#include <caml/signals.h>


void _bs_posix_fadvise(value fd, value offset, value len, value advice) {
        int ret = 0;

        int c_fd = -1;
        off_t c_offset = 0;
        off_t c_len = 0;
        int c_advice = 0;

        CAMLparam4(fd, offset, len, advice);

        c_fd = Int_val(fd);
        c_offset = Long_val(offset);
        c_len = Long_val(len);

        switch(Int_val(advice)) {
                case 0:
                        c_advice = POSIX_FADV_NORMAL;
                        break;
                case 1:
                        c_advice = POSIX_FADV_SEQUENTIAL;
                        break;
                case 2:
                        c_advice = POSIX_FADV_RANDOM;
                        break;
                case 3:
                        c_advice = POSIX_FADV_NOREUSE;
                        break;
                case 4:
                        c_advice = POSIX_FADV_WILLNEED;
                        break;
                case 5:
                        c_advice = POSIX_FADV_DONTNEED;
                        break;
                default:
                        caml_invalid_argument("advice");
                        break;
        }

        enter_blocking_section();
          ret = posix_fadvise(c_fd, c_offset, c_len, c_advice);
        leave_blocking_section();

        if(ret != 0) {
                uerror("posix_fadvise", Nothing);
        }

        CAMLreturn0;
}

void _bs_posix_fallocate(value fd, value mode, value offset,
                         value len) {
  int ret = 0;

  int c_fd = -1;
  int c_mode = 0;
  off_t c_offset = 0;
  off_t c_len = 0;

  CAMLparam4(fd, mode, offset, len);

  c_fd = Int_val(fd);
  c_mode = Int_val(mode);
  c_offset = Long_val(offset);
  c_len = Long_val(len);

  ret = fallocate(c_fd, c_mode, c_offset, c_len);

  if(ret < 0) {
    uerror("fallocate", Nothing);
  }

  CAMLreturn0;
}
