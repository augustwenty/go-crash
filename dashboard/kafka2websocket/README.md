# Build for MacOS
* install `openssl` with `brew install openssl`
* run
```
cd k2ws
CFLAGS="-I/usr/local/opt/openssl/include"
LDFLAGS="-L/usr/local/opt/openssl/lib"
go build -tags static
```

# Run it
`./k2ws`

# See if it works
you should be able to see the test page at http://localhost:8080


# More info
See OLDREADME.md