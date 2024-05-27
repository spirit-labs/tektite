To generate a server cert and key for development on 127.0.0.1 use the following command line. Note that days must
be <= 825 on modern MacOS due to a new Apple restriction

openssl req -new -newkey rsa:2048 -days 825 -nodes -x509 -keyout server.key -out server.crt -config openssl.cnf

