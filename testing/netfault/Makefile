# Builds faulteron's BPF program, for running bin/faulteron on a Linux host without docker; the image builds it
# here too. Building needs clang, libbpf's headers (libbpf-dev) and the kernel's UAPI headers (linux-libc-dev);
# running needs root, bpftool, iproute2 and jq.
#
#   make                        build/faulteron.bpf.o, which bin/faulteron finds from a checkout
#   sudo make install           bin/faulteron and the program under PREFIX, /usr/local by default
#   sudo make uninstall
#   make clean

PREFIX ?= /usr/local
CLANG ?= clang
# Debian and Ubuntu keep asm/types.h under the multiarch include directory; elsewhere it is not there and harmless.
BPF_CFLAGS := -O2 -g -Wall -Werror -target bpf -I/usr/include/$(shell uname -m)-linux-gnu

all: build/faulteron.bpf.o

build/faulteron.bpf.o: bpf/faulteron.bpf.c
	@mkdir -p build
	$(CLANG) $(BPF_CFLAGS) -c $< -o $@

install: build/faulteron.bpf.o
	install -D -m 755 bin/faulteron $(DESTDIR)$(PREFIX)/bin/faulteron
	install -D -m 644 build/faulteron.bpf.o $(DESTDIR)$(PREFIX)/lib/faulteron/faulteron.bpf.o

uninstall:
	rm -f $(DESTDIR)$(PREFIX)/bin/faulteron
	rm -rf $(DESTDIR)$(PREFIX)/lib/faulteron

clean:
	rm -rf build

.PHONY: all install uninstall clean
