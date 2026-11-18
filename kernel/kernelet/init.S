// SPDX-License-Identifier: MPL-2.0
// A real minimal PID 1 for bootstrap validation; no benchmark is run here.
.section .text
.global _start
_start:
    mov $1, %eax
    mov $1, %edi
    lea message(%rip), %rsi
    mov $message_end-message, %edx
    syscall
1:
    mov $34, %eax
    syscall
    jmp 1b
.section .rodata
message:
    .ascii "Asterinas Kernelet: userspace init reached; waiting for signals.\n"
message_end:
.section .note.GNU-stack,"",@progbits
