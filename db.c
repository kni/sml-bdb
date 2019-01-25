#include <db.h>
#include <string.h>


// int db_create(DB **dbp, DB_ENV *dbenv, u_int32_t flags);


int db_set_re_len(DB *db, u_int32_t re_len) {
	return db->set_re_len(db, re_len);
}


int db_open(DB *db, DB_TXN *txnid, char *filename, char *dbname, DBTYPE type, u_int32_t flags, int mode) {
	return db->open(db, txnid, filename, dbname, type, flags, mode);
}


int db_close(DB *db, u_int32_t flags) {
	return db->close(db, flags);
}



int db_put(DB *db, DB_TXN *txnid, const char *key, u_int32_t key_len, const char *data, u_int32_t data_len, u_int32_t flags) {
	DBT dbt_key, dbt_data;

	memset(&dbt_key, 0, sizeof(dbt_key));
	dbt_key.data = (void *) key;
	dbt_key.size = key_len;

	memset(&dbt_data, 0, sizeof(dbt_data));
	dbt_data.data = (void *) data;
	dbt_data.size = data_len;

	return db->put(db, txnid, &dbt_key, &dbt_data, flags);
}


int db_put_recno(DB *db, DB_TXN *txnid, db_recno_t *key, const char *data, u_int32_t data_len, u_int32_t flags) {
	DBT dbt_key, dbt_data;

	memset(&dbt_key, 0, sizeof(dbt_key));
	dbt_key.data = (void *) key;
	dbt_key.size = sizeof(db_recno_t);

	memset(&dbt_data, 0, sizeof(dbt_data));
	dbt_data.data = (void *) data;
	dbt_data.size = data_len;

	int ret = db->put(db, txnid, &dbt_key, &dbt_data, flags);

	*key = *(u_long *) dbt_key.data;
	return ret;
}



int db_get(DB *db, DB_TXN *txnid, const char *key, u_int32_t key_len, char **data, u_int32_t *data_len, u_int32_t flags) {
	DBT dbt_key, dbt_data;

	memset(&dbt_key, 0, sizeof(dbt_key));
	dbt_key.data = (void *) key;
	dbt_key.size = key_len;

	memset(&dbt_data, 0, sizeof(dbt_data));

	int ret = db->get(db, txnid, &dbt_key, &dbt_data, flags);

	if (ret == 0) {
		*data = (char *) dbt_data.data;
		*data_len = dbt_data.size;
	} else {
		*data = NULL;
		*data_len = 0;
	}
	return ret;
}


int db_get_recno(DB *db, DB_TXN *txnid, db_recno_t key, char **data, u_int32_t *data_len, u_int32_t flags) {
	DBT dbt_key, dbt_data;

	memset(&dbt_key, 0, sizeof(dbt_key));
	dbt_key.data = &key;
	dbt_key.size = sizeof(db_recno_t);

	memset(&dbt_data, 0, sizeof(dbt_data));

	int ret = db->get(db, txnid, &dbt_key, &dbt_data, flags);

	if (ret == 0) {
		*data = (char *) dbt_data.data;
		*data_len = dbt_data.size;
	} else {
		*data = NULL;
		*data_len = 0;
	}
	return ret;
}



int db_exists(DB *db, DB_TXN *txnid, const char *key, u_int32_t key_len, u_int32_t flags) {
	DBT dbt_key;

	memset(&dbt_key, 0, sizeof(dbt_key));
	dbt_key.data = (void *) key;
	dbt_key.size = key_len;

	return db->exists(db, txnid, &dbt_key, flags);
}


int db_exists_recno(DB *db, DB_TXN *txnid, db_recno_t key, u_int32_t flags) {
	DBT dbt_key;

	memset(&dbt_key, 0, sizeof(dbt_key));
	dbt_key.data = &key;
	dbt_key.size = sizeof(db_recno_t);

	return db->exists(db, txnid, &dbt_key, flags);
}



int db_del(DB *db, DB_TXN *txnid, const char *key, u_int32_t key_len, u_int32_t flags) {
	DBT dbt_key;

	memset(&dbt_key, 0, sizeof(dbt_key));
	dbt_key.data = (void *) key;
	dbt_key.size = key_len;

	return db->del(db, txnid, &dbt_key, flags);
}


int db_del_recno(DB *db, DB_TXN *txnid, db_recno_t key, u_int32_t flags) {
	DBT dbt_key;

	memset(&dbt_key, 0, sizeof(dbt_key));
	dbt_key.data = &key;
	dbt_key.size = sizeof(db_recno_t);

	return db->del(db, txnid, &dbt_key, flags);
}
