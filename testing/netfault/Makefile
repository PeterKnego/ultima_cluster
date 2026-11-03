# Builds netfault's BPF program, for running bin/netfault on a Linux host without docker; the image builds it here
# too. Building needs clang, libbpf's headers (libbpf-dev) and the kernel's UAPI headers (linux-libc-dev); running
# needs root, bpftool, iproute2 and jq.
#
#   make                        build/netfault.bpf.o, which bin/netfault finds from a checkout
#   sudo make install           bin/netfault and the program under PREFIX, /usr/local by default
#   sudo make uninstall
#   make clean

PREFIX ?= /usr/local
CLANG ?= clang
# Debian and Ubuntu keep asm/types.h under the multiarch include directory; elsewhere it is not there and harmless.
BPF_CFLAGS := -O2 -g -Wall -Werror -target bpf -I/usr/include/$(shell uname -m)-linux-gnu

all: build/netfault.bpf.o

build/netfault.bpf.o: bpf/netfault.bpf.c
	@mkdir -p build
	$(CLANG) $(BPF_CFLAGS) -c $< -o $@

install: build/netfault.bpf.o
	install -D -m 755 bin/netfault $(DESTDIR)$(PREFIX)/bin/netfault
	install -D -m 644 build/netfault.bpf.o $(DESTDIR)$(PREFIX)/lib/netfault/netfault.bpf.o

uninstall:
	rm -f $(DESTDIR)$(PREFIX)/bin/netfault
	rm -rf $(DESTDIR)$(PREFIX)/lib/netfault

clean:
	rm -rf build

.PHONY: all install uninstall clean
