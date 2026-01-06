package main

import (
	"crypto/md5"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"fmt"
	"net"
	"os"

	"github.com/refractionPOINT/go-limacharlie/limacharlie"
)

var nonRootCA = map[string]struct{}{
	"lc": struct{}{},
}

func main() {
	if len(os.Args) != 2 {
		panic("usage: connectivity <oid>")
	}
	oid := os.Args[1]
	fmt.Printf("oid: %s\n", oid)

	// Get the relevant site.
	o, err := limacharlie.NewOrganizationFromClientOptions(limacharlie.ClientOptions{
		OID: oid,
	}, &limacharlie.LCLoggerZerolog{})
	if err != nil {
		panic(err)
	}
	connInfo, err := o.GetSiteConnectivityInfo()
	if err != nil {
		panic(err)
	}

	for urlName, url := range connInfo.URLs.ToMap() {
		// Resolve the relevant DNS.
		recs, err := net.LookupIP(url)
		if err != nil {
			fmt.Printf("!!    failed looking up URL for %q: %v\n", url, err)
			continue
		}
		if len(recs) == 0 {
			fmt.Printf("!!    no IPs for URL %q\n", url)
			continue
		}
		fmt.Printf("\nURL %q: ", url)
		for _, rec := range recs {
			fmt.Printf("%v  ", rec)
		}
		fmt.Printf("\n---------------------------------\n")

		// TCP Connect to the relevant domains.
		if err := testTCPConnect(url); err != nil {
			fmt.Printf("!!    failed TCP connect on port 443 to %q: %v\n", url, err)
			continue
		} else {
			fmt.Printf("OK    TCP connect on port 443 to %q succeeded\n", url)
		}

		// Get the SSL certificate.
		certs, err := testSSLConnect(url, false)
		if err != nil {
			fmt.Printf("!!    failed SSL connect on port 443 to %q: %v\n", url, err)
			continue
		} else {
			fmt.Printf("OK    SSL connect on port 443 to %q succeeded\n", url)
		}
		if len(certs) == 0 {
			fmt.Printf("!!    no SSL certificates for URL %q\n", url)
			continue
		}
		for _, cert := range certs {
			fingerprint := md5.Sum(cert.Raw)
			fingerprintStr := hex.EncodeToString(fingerprint[:])
			fmt.Printf("OK    SSL certificate for %q: %s\n", url, fingerprintStr)
			if _, ok := nonRootCA[urlName]; ok {
				expectedFP, ok := connInfo.Certs[url]
				if !ok {
					// We don't expect this to be verifiable.
					fmt.Printf("??    SSL certificate for %q is not verifiable (non-root CA), check manually\n", url)
				} else {
					if expectedFP != fingerprintStr {
						fmt.Printf("!!    SSL certificate for %q does not match expected fingerprint %s\n", url, fingerprintStr)
					} else {
						fmt.Printf("OK    SSL certificate for %q matches expected fingerprint %s\n", url, fingerprintStr)
					}
				}
			} else {
				// Verify the certificate.
				if _, err := testSSLConnect(url, true); err != nil {
					fmt.Printf("!!    failed SSL verify %q: %v\n", url, err)
					continue
				} else {
					fmt.Printf("OK    SSL connect on port 443 to %q verified\n", url)
				}
			}
		}
	}
}

func testTCPConnect(a string) error {
	c, err := net.Dial("tcp", fmt.Sprintf("%s:443", a))
	if err != nil {
		return err
	}
	defer c.Close()
	return nil
}

func testSSLConnect(a string, isVerify bool) ([]*x509.Certificate, error) {
	c, err := tls.Dial("tcp", fmt.Sprintf("%s:443", a), &tls.Config{
		InsecureSkipVerify: !isVerify,
	})
	if err != nil {
		return nil, err
	}
	defer c.Close()
	return c.ConnectionState().PeerCertificates, nil
}
