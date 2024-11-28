// Copyright (c) 2018 - 2020 Blockwatch Data Inc.
// Author: alex@blockwatch.cc

package util

import (
	"net"
	"net/url"
	"os"
	"path"
	"strings"
)

func Fqdn() string {
	hostname, err := os.Hostname()
	if err != nil {
		return "unknown"
	}

	addrs, err := net.LookupIP(hostname)
	if err != nil {
		return hostname
	}

	for _, addr := range addrs {
		if ipv4 := addr.To4(); ipv4 != nil {
			ip, err := ipv4.MarshalText()
			if err != nil {
				return hostname
			}
			hosts, err := net.LookupAddr(string(ip))
			if err != nil || len(hosts) == 0 {
				return hostname
			}
			fqdn := hosts[0]
			return strings.TrimSuffix(fqdn, ".") // return fqdn without trailing dot
		}
	}
	return hostname
}

func Basename(s string) string {
	if u, err := url.Parse(s); err == nil {
		return path.Base(u.Path)
	}
	f := strings.Split(strings.Split(strings.Split(s, "?")[0], "#")[0], "/")
	return f[len(f)-1]
}
