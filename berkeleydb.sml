structure BerkeleyDB :
sig
  exception BerkeleyDB of int
  type dbTxn
  datatype dbFlag = DB_CREATE | DB_RECOVER | DB_APPEND

  structure BTree :
  sig
    type db
    val dbCreate : dbTxn option -> db

    val dbOpen   : db * string option * string option * dbFlag list * int -> unit
    val dbClose  : db * dbFlag list -> unit

    val dbPut    : db * string * string * dbFlag list -> unit
    val dbGet    : db * string * dbFlag list -> string option
    val dbExists : db * string * dbFlag list -> bool
    val dbDel    : db * string * dbFlag list -> unit
  end

  structure Hash :
  sig
    type db
    val dbCreate : dbTxn option -> db

    val dbOpen   : db * string option * string option * dbFlag list * int -> unit
    val dbClose  : db * dbFlag list -> unit

    val dbPut    : db * string * string * dbFlag list -> unit
    val dbGet    : db * string * dbFlag list -> string option
    val dbExists : db * string * dbFlag list -> bool
    val dbDel    : db * string * dbFlag list -> unit
  end

  structure Recno :
  sig
    type db
    val dbCreate : dbTxn option -> db
    val dbSetReLen : db * int -> unit

    val dbOpen   : db * string option * string option * dbFlag list * int -> unit
    val dbClose  : db * dbFlag list -> unit

    val dbPut    : db * int * string * dbFlag list -> int
    val dbGet    : db * int * dbFlag list -> string option
    val dbExists : db * int * dbFlag list -> bool
    val dbDel    : db * int * dbFlag list -> unit
  end

  structure Queue :
  sig
    type db
    val dbCreate : dbTxn option -> db
    val dbSetReLen : db * int -> unit

    val dbOpen   : db * string option * string option * dbFlag list * int -> unit
    val dbClose  : db * dbFlag list -> unit

    val dbPut    : db * int * string * dbFlag list -> int
    val dbGet    : db * int * dbFlag list -> string option
    val dbExists : db * int * dbFlag list -> bool
    val dbDel    : db * int * dbFlag list -> unit
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

    fun dbCreate dbtxn : db = BTree (db_create (dbTxn dbtxn))

    fun dbOpen (BTree (db, dbtxn), filename, dbname, flags, mode) =
      db_open (db, dbtxn, filename, dbname, (dbTypeToInt BTREE), (dbFlagsAnd flags), mode)

    fun dbClose  (BTree (db, dbtxn), flags)            = db_close  (db, dbFlagsAnd flags)
    fun dbPut    (BTree (db, dbtxn), key, data, flags) = db_put    (db, dbtxn, key, data, dbFlagsAnd flags)
    fun dbGet    (BTree (db, dbtxn), key, flags)       = db_get    (db, dbtxn, key, dbFlagsAnd flags)
    fun dbExists (BTree (db, dbtxn), key, flags)       = db_exists (db, dbtxn, key, dbFlagsAnd flags)
    fun dbDel    (BTree (db, dbtxn), key, flags)       = db_del    (db, dbtxn, key, dbFlagsAnd flags)
  end


  structure Hash =
  struct
    open Hash

    fun dbCreate dbtxn = Hash (db_create (dbTxn dbtxn))

    fun dbOpen (Hash (db, dbtxn), filename, dbname, flags, mode) =
      db_open (db, dbtxn, filename, dbname, (dbTypeToInt HASH), (dbFlagsAnd flags), mode)

    fun dbClose  (Hash (db, dbtxn), flags)            = db_close  (db, dbFlagsAnd flags)
    fun dbPut    (Hash (db, dbtxn), key, data, flags) = db_put    (db, dbtxn, key, data, dbFlagsAnd flags)
    fun dbGet    (Hash (db, dbtxn), key, flags)       = db_get    (db, dbtxn, key, dbFlagsAnd flags)
    fun dbExists (Hash (db, dbtxn), key, flags)       = db_exists (db, dbtxn, key, dbFlagsAnd flags)
    fun dbDel    (Hash (db, dbtxn), key, flags)       = db_del    (db, dbtxn, key, dbFlagsAnd flags)
  end


  structure Recno =
  struct
    open Recno

    fun dbCreate dbtxn = Recno (db_create (dbTxn dbtxn))

    fun dbOpen (Recno (db, dbtxn), filename, dbname, flags, mode) =
      db_open (db, dbtxn, filename, dbname, (dbTypeToInt RECNO), (dbFlagsAnd flags), mode)

    fun dbSetReLen (Recno (db, dbtxn), len) = db_set_re_len (db, len)

    fun dbClose  (Recno (db, dbtxn), flags)            = db_close        (db, dbFlagsAnd flags)
    fun dbPut    (Recno (db, dbtxn), key, data, flags) = db_put_recno    (db, dbtxn, key, data, dbFlagsAnd flags)
    fun dbGet    (Recno (db, dbtxn), key, flags)       = db_get_recno    (db, dbtxn, key, dbFlagsAnd flags)
    fun dbExists (Recno (db, dbtxn), key, flags)       = db_exists_recno (db, dbtxn, key, dbFlagsAnd flags)
    fun dbDel    (Recno (db, dbtxn), key, flags)       = db_del_recno    (db, dbtxn, key, dbFlagsAnd flags)
  end


  structure Queue =
  struct
    open Queue

    fun dbCreate dbtxn = Queue (db_create (dbTxn dbtxn))

    fun dbOpen (Queue (db, dbtxn), filename, dbname, flags, mode) =
      db_open (db, dbtxn, filename, dbname, (dbTypeToInt QUEUE), (dbFlagsAnd flags), mode)

    fun dbSetReLen (Queue (db, dbtxn), len) = db_set_re_len (db, len)

    fun dbClose  (Queue (db, dbtxn), flags)            = db_close        (db, dbFlagsAnd flags)
    fun dbPut    (Queue (db, dbtxn), key, data, flags) = db_put_recno    (db, dbtxn, key, data, dbFlagsAnd flags)
    fun dbGet    (Queue (db, dbtxn), key, flags)       = db_get_recno    (db, dbtxn, key, dbFlagsAnd flags)
    fun dbExists (Queue (db, dbtxn), key, flags)       = db_exists_recno (db, dbtxn, key, dbFlagsAnd flags)
    fun dbDel    (Queue (db, dbtxn), key, flags)       = db_del_recno    (db, dbtxn, key, dbFlagsAnd flags)
  end

end
