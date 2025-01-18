OPENSSL = openssl

%.key:
	$(OPENSSL) ecparam -genkey -name prime256v1 -out "$@"

clean:
	rm *.key *.crt *.prof

ca.crt: ca.key
	$(OPENSSL) req -new -x509 -days 30 -key "$<" -out "$@" \
		-subj "/C=FR/L=Paris/O=GrintaLovers/CN=GrintaCA"

leaf.crt: ca.crt ca.key leaf.key
	$(OPENSSL) req -new -key leaf.key \
		-subj "/C=FR/L=Paris/O=GrintaLovers/CN=GrintaNode" \
		-addext "subjectAltName = IP:127.0.0.1, IP:::" \
		| $(OPENSSL) x509 -req -CA ca.crt -CAkey ca.key -out "$@" -days 7 \
		-copy_extensions copy

run-single: ca.crt leaf.crt leaf.key
	go run . -tls-ca ca.crt -tls-cert leaf.crt -tls-key leaf.key \
		-hostname "node1" -client -server

run-client: ca.crt leaf.crt leaf.key
	go run . -tls-ca ca.crt -tls-cert leaf.crt -tls-key leaf.key \
		-hostname "node1" -client -port 6001 -neighbours localhost:6002

run-server: ca.crt leaf.crt leaf.key
	go run . -tls-ca ca.crt -tls-cert leaf.crt -tls-key leaf.key \
		-hostname "node2" -server -port 6002 \
