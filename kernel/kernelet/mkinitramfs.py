#!/usr/bin/env python3
# SPDX-License-Identifier: MPL-2.0
"""Pack a real init ELF in a deterministic uncompressed newc archive."""
import pathlib
import struct
import sys


def pack(init, output):
    elf = pathlib.Path(init).read_bytes()
    if elf[:5] != b'\x7fELF\x02' or struct.unpack_from('<H', elf, 18)[0] != 62:
        raise ValueError('init must be an x86-64 ELF executable')
    archive = bytearray()
    for ino, (name, mode, data) in enumerate([
        ('dev', 0o040755, b''),
        ('proc', 0o040555, b''),
        ('sys', 0o040555, b''),
        ('tmp', 0o041777, b''),
        ('init', 0o100755, elf),
        ('TRAILER!!!', 0, b''),
    ], 1):
        encoded = name.encode() + b'\0'
        fields = (ino, mode, 0, 0, 1, 0, len(data), 0, 0, 0, 0, len(encoded), 0)
        archive += b'070701' + ''.join(f'{n:08x}' for n in fields).encode()
        archive += encoded
        archive += bytes(-len(archive) % 4)
        archive += data
        archive += bytes(-len(archive) % 4)
    pathlib.Path(output).write_bytes(archive)


if __name__ == '__main__':
    pack(*sys.argv[1:])
