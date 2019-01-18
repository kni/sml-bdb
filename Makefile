all:
	@echo "target: poly mlton clean"

poly:
	cc -shared -I/usr/local/include/db5 -L/usr/local/lib -ldb-5 -o db.so db.c
	polyc -o t-poly t.mlp
	env LD_LIBRARY_PATH=. ./t-poly

mlton:
	mlton -default-ann 'allowFFI true' -cc-opt -I/usr/local/include/db5 -link-opt -ldb-5 -output t-mlton t.mlb db.c
	./t-mlton

clean:
	rm -f t-poly t-mlton db.so
