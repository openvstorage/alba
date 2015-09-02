#!/bin/bash -xue

which opam > /dev/null || { echo 'opam not found!'; exit 1; }

opam switch 4.02.1
eval `opam config env`

opam update

# do this in 1 step, otherwise, you might not get what you want
opam install -y ssl.0.5.0 \
     camlbz2.0.6.0 \
     snappy.0.1.0 \
     lwt.2.5.0 \
     kinetic-client.0.0.6 \
     camltc.0.9.2 \
     cstruct.1.7.0 \
     ctypes.0.4.1 \
     ctypes-foreign.0.4.0 \
     bisect.1.3 \
     ocplib-endian.0.8 \
     quickcheck.1.0.2 \
     nocrypto.0.5.1 \
     uuidm.0.9.5 \
     zarith.1.3 \
     sexplib.112.35.00 \
     core_kernel.112.35.00
