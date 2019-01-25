structure BerkeleyDB :
sig
  exception BerkeleyDB of int
  type dbTxn
  datatype dbFlag = DB_CREATE | DB_RECOVER | DB_APPEND

  structure BTree :
  sig
    type db
    val dbOpen   : dbTxn option * string option * string option * dbFlag list * int -> db
    val dbClose  : db * dbFlag list -> unit

    val dbPut    : db * dbTxn option * string * string * dbFlag list -> unit
    val dbGet    : db * dbTxn option * string * dbFlag list -> string option
    val dbExists : db * dbTxn option * string * dbFlag list -> bool
    val dbDel    : db * dbTxn option * string * dbFlag list -> unit
  end

  structure Hash :
  sig
    type db
    val dbOpen   : dbTxn option * string option * string option * dbFlag list * int -> db
    val dbClose  : db * dbFlag list -> unit

    val dbPut    : db * dbTxn option * string * string * dbFlag list -> unit
    val dbGet    : db * dbTxn option * string * dbFlag list -> string option
    val dbExists : db * dbTxn option * string * dbFlag list -> bool
    val dbDel    : db * dbTxn option * string * dbFlag list -> unit
  end

  structure Recno :
  sig
    type db
    val dbOpen   : dbTxn option * string option * string option * dbFlag list * int -> db
    val dbClose  : db * dbFlag list -> unit

    val dbPut    : db * dbTxn option * int * string * dbFlag list -> int
    val dbGet    : db * dbTxn option * int * dbFlag list -> string option
    val dbExists : db * dbTxn option * int * dbFlag list -> bool
    val dbDel    : db * dbTxn option * int * dbFlag list -> unit
  end

  structure Queue :
  sig
    type db
    val dbOpen   : dbTxn option * string option * string option * dbFlag list * int -> db
    val dbClose  : db * dbFlag list -> unit

    val dbPut    : db * dbTxn option * int * string * dbFlag list -> int
    val dbGet    : db * dbTxn option * int * dbFlag list -> string option
    val dbExists : db * dbTxn option * int * dbFlag list -> bool
    val dbDel    : db * dbTxn option * int * dbFlag list -> unit
  end

end
=
struct
  open BerkeleyDB

  fun dbTxn NONE = NONE
    | dbTxn (SOME (DB_TXN dbtxn)) = SOME dbtxn

  datatype dbType = BTREE | HASH | RECNO | QUEUE

  fun dbTypeToInt BTREE = 1
    | dbTypeToInt HASH  = 2
    | dbTypeToInt RECNO = 3
    | dbTypeToInt QUEUE = 4


  datatype dbFlag = DB_CREATE | DB_RECOVER | DB_APPEND

  fun dbFlagToWord DB_CREATE  = 0wx1
    | dbFlagToWord DB_RECOVER = 0wx2
    | dbFlagToWord DB_APPEND  = 0wx2

  fun dbFlagsAnd flags = Word.toInt (List.foldl (fn (v,a) => Word.orb (dbFlagToWord v, a)) 0w0 flags)



  structure BTree =
  struct
    open BTree

    fun dbOpen (dbtxn, filename, dbname, flags, mode) =
      BTree (db_open (dbTxn dbtxn, filename, dbname, (dbTypeToInt BTREE), (dbFlagsAnd flags), mode))

    fun dbClose  (BTree db, flags)                   = db_close  (db, dbFlagsAnd flags)
    fun dbPut    (BTree db, dbtxn, key, data, flags) = db_put    (db, dbTxn dbtxn, key, data, dbFlagsAnd flags)
    fun dbGet    (BTree db, dbtxn, key, flags)       = db_get    (db, dbTxn dbtxn, key, dbFlagsAnd flags)
    fun dbExists (BTree db, dbtxn, key, flags)       = db_exists (db, dbTxn dbtxn, key, dbFlagsAnd flags)
    fun dbDel    (BTree db, dbtxn, key, flags)       = db_del    (db, dbTxn dbtxn, key, dbFlagsAnd flags)
  end


  structure Hash =
  struct
    open Hash

    fun dbOpen (dbtxn, filename, dbname, flags, mode) =
      Hash (db_open (dbTxn dbtxn, filename, dbname, (dbTypeToInt HASH), (dbFlagsAnd flags), mode))

    fun dbClose  (Hash db, flags)                   = db_close  (db, dbFlagsAnd flags)
    fun dbPut    (Hash db, dbtxn, key, data, flags) = db_put    (db, dbTxn dbtxn, key, data, dbFlagsAnd flags)
    fun dbGet    (Hash db, dbtxn, key, flags)       = db_get    (db, dbTxn dbtxn, key, dbFlagsAnd flags)
    fun dbExists (Hash db, dbtxn, key, flags)       = db_exists (db, dbTxn dbtxn, key, dbFlagsAnd flags)
    fun dbDel    (Hash db, dbtxn, key, flags)       = db_del    (db, dbTxn dbtxn, key, dbFlagsAnd flags)
  end


  structure Recno =
  struct
    open Recno

    fun dbOpen (dbtxn, filename, dbname, flags, mode) =
      Recno (db_open (dbTxn dbtxn, filename, dbname, (dbTypeToInt RECNO), (dbFlagsAnd flags), mode))

    fun dbClose  (Recno db, flags)                   = db_close        (db, dbFlagsAnd flags)
    fun dbPut    (Recno db, dbtxn, key, data, flags) = db_put_recno    (db, dbTxn dbtxn, key, data, dbFlagsAnd flags)
    fun dbGet    (Recno db, dbtxn, key, flags)       = db_get_recno    (db, dbTxn dbtxn, key, dbFlagsAnd flags)
    fun dbExists (Recno db, dbtxn, key, flags)       = db_exists_recno (db, dbTxn dbtxn, key, dbFlagsAnd flags)
    fun dbDel    (Recno db, dbtxn, key, flags)       = db_del_recno    (db, dbTxn dbtxn, key, dbFlagsAnd flags)
  end


  structure Queue =
  struct
    open Queue

    fun dbOpen (dbtxn, filename, dbname, flags, mode) =
      Queue (db_open (dbTxn dbtxn, filename, dbname, (dbTypeToInt QUEUE), (dbFlagsAnd flags), mode))

    fun dbClose  (Queue db, flags)                   = db_close        (db, dbFlagsAnd flags)
    fun dbPut    (Queue db, dbtxn, key, data, flags) = db_put_recno    (db, dbTxn dbtxn, key, data, dbFlagsAnd flags)
    fun dbGet    (Queue db, dbtxn, key, flags)       = db_get_recno    (db, dbTxn dbtxn, key, dbFlagsAnd flags)
    fun dbExists (Queue db, dbtxn, key, flags)       = db_exists_recno (db, dbTxn dbtxn, key, dbFlagsAnd flags)
    fun dbDel    (Queue db, dbtxn, key, flags)       = db_del_recno    (db, dbTxn dbtxn, key, dbFlagsAnd flags)
  end

end
