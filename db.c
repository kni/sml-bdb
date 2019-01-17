#include <db.h>


// int db_create(DB **dbp, DB_ENV *dbenv, u_int32_t flags);


int db_open(DB *dbp, DB_TXN *txnid, char *filename, char *dbname, DBTYPE type, u_int32_t flags, int mode) {
	return dbp->open(dbp, txnid, filename, dbname, type, flags, mode);
}


int db_close(DB *db, u_int32_t flags) {
	return db->close(db, flags);
}
