OPENSSL = openssl

%.key:
	$(OPENSSL) ecparam -genkey -name prime256v1 -out "$@"

ca.crt: ca.key
	$(OPENSSL) req -new -x509 -days 30 -key "$<" -out "$@" \
		-subj "/C=FR/L=Paris/O=GrintaLovers/CN=GrintaCA"

leaf.crt: ca.crt ca.key leaf.key
	$(OPENSSL) req -new -key leaf.key \
		-subj "/C=FR/L=Paris/O=GrintaLovers/CN=GrintaNode" \
		-addext "subjectAltName = IP:127.0.0.1, IP:::" \
		| $(OPENSSL) x509 -req -CA ca.crt -CAkey ca.key -out "$@" -days 7 \
		-copy_extensions copy

run-single-node1: ca.crt leaf.crt leaf.key
	go run . -tls-ca ca.crt -tls-cert leaf.crt -tls-key leaf.key \
		-hostname "node1" -until 6 -max 5 -port 6001

run-cluster-node1: ca.crt leaf.crt leaf.key
	go run . -tls-ca ca.crt -tls-cert leaf.crt -tls-key leaf.key \
		-hostname "node1" -until 6 -max 15 -port 6001

run-cluster-node2: ca.crt leaf.crt leaf.key
	go run . -tls-ca ca.crt -tls-cert leaf.crt -tls-key leaf.key \
		-hostname "node2" -from 6 -until 11 -max 15 -port 6002 \
		-neighbours localhost:6001

run-cluster-node3: ca.crt leaf.crt leaf.key
	go run . -tls-ca ca.crt -tls-cert leaf.crt -tls-key leaf.key \
		-hostname "node3" -from 11 -until 16 -max 15 -port 6003 \
		-neighbours localhost:6002
