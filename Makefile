all:
	cc -shared -I/usr/local/include/db5 -L/usr/local/lib -ldb-5 -o db.so db.c
