# Full Asterinas on Kernelet

Build from the sibling Kernelet checkout using `make -C prototype KLET_KERNEL=asterinas image` after sourcing the task-local tool environment. This links the original Asterinas assembler and `aster_core::boot`, not a replacement test kernel. Both repositories must be checked out as sibling directories named `asterinas` and `kernelet`.

The default archive contains a real minimal static ELF `/init` that prints to the console and waits for signals. It is bootstrap userspace, not a Linux distribution or a communication test. Set the absolute `KLET_INITRAMFS` Make variable to another real CPIO or gzip CPIO archive for subsequent functional experiments. The final combined image must still fit the unchanged 16 MiB loader limit.

Only the Kernelet platform and TypedIO transport are selected. Physical ACPI, PCI, VirtIO, framebuffer, CMOS and host power-control paths are excluded; filesystem, process, syscall, scheduler, loopback and TypedIO socket initialization use their original component stages and order. The host console currently supports output only. Wall time starts from the upstream fallback RTC epoch, while monotonic time uses the authenticated host TSC frequency.

Component, kernel-parameter and instance-power registrations are explicitly constructed on the BSP after Host Services and memory initialization and before command-line processing. The function table is private relocated data. The ELF loader does not execute code at registration time or accept automatic ELF constructors. Component initialization still occurs at its original Bootstrap/Kthread/Process stages.

The final build uses panic=abort. Fatal panics terminate the instance; they do not unwind through Linux Host Services. QEMU/KVM startup, first userspace execution, multiprocessor IRQ/TLS/futex behavior and real TypedIO communication require later runtime verification and have not been demonstrated by compilation or native fixtures.
