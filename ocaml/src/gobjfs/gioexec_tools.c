#define _GNU_SOURCE

#include <caml/alloc.h>
#include <fcntl.h>


static int open_flag_table[15] = {
  O_RDONLY, O_WRONLY, O_RDWR, O_NONBLOCK, O_APPEND, O_CREAT, O_TRUNC, O_EXCL,
  O_NOCTTY, O_DSYNC, O_SYNC, O_RSYNC,
  0, /* O_SHARE_DELETE, Windows-only */
  O_CLOEXEC,
  O_DIRECT
};

CAMLprim value gobjfs_ocaml_convert_open_flags(value flags){
  int cv_flags = convert_flag_list(flags, open_flag_table);
  return Val_int(cv_flags);
  
}
