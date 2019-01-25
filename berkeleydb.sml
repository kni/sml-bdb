structure BerkeleyDB :
sig
  exception BerkeleyDB of int
  type dbTxn
  datatype dbFlag = DB_CREATE | DB_RECOVER | DB_APPEND

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

end
=
struct
  open BerkeleyDB

  fun dbTxn NONE = NONE
    | dbTxn (SOME (DB_TXN dbtxn)) = SOME dbtxn

  datatype dbType = BTREE | HASH | RECNO | QUEUE

  fun dbTypeToInt BTREE   = 1
    | dbTypeToInt HASH    = 2
    | dbTypeToInt RECNO   = 3
    | dbTypeToInt QUEUE   = 4


  datatype dbFlag = DB_CREATE | DB_RECOVER | DB_APPEND

  fun dbFlagToWord DB_CREATE  = 0wx1
    | dbFlagToWord DB_RECOVER = 0wx2
    | dbFlagToWord DB_APPEND  = 0wx2

  fun dbFlagsAnd flags = Word.toInt (List.foldl (fn (v,a) => Word.orb (dbFlagToWord v, a)) 0w0 flags)


  fun dbOpen' (dbtxn, filename, dbname, dbtype, flags, mode) =
    let
      val db = db_create ()
      val r = db_open (db, dbTxn dbtxn, filename, dbname, (dbTypeToInt dbtype), (dbFlagsAnd flags), mode)
    in
      if r = 0 then db else raise BerkeleyDB r
    end


  fun dbClose' (db, flags) =
    let
      val r = db_close (db, (dbFlagsAnd flags))
    in
      if r = 0 then () else raise BerkeleyDB r
    end


  fun dbPut' (db, dbtxn, key, data, flags) =
    let
      val r = db_put (db, dbTxn dbtxn, key, String.size key, data, String.size data, dbFlagsAnd flags)
    in
      if r = 0 then () else raise BerkeleyDB r
    end


  fun dbPutRecno' (db, dbtxn, key, data, flags) = db_put_recno (db, dbTxn dbtxn, key, data, String.size data, dbFlagsAnd flags)


  fun dbGet' (db, dbtxn, key, flags) = db_get (db, dbTxn dbtxn, key, String.size key, dbFlagsAnd flags)


  fun dbGetRecno' (db, dbtxn, key, flags) = db_get_recno (db, dbTxn dbtxn, key, dbFlagsAnd flags)


  fun dbExists' (db, dbtxn, key, flags) =
    let
      val r = db_exists (db, dbTxn dbtxn, key, String.size key, dbFlagsAnd flags)
    in
      if r = 0 then true else
      if isAbsent r then false else raise BerkeleyDB r
    end


  fun dbExistsRecno' (db, dbtxn, key, flags) =
    let
      val r = db_exists_recno (db, dbTxn dbtxn, key, dbFlagsAnd flags)
    in
      if r = 0 then true else
      if isAbsent r then false else raise BerkeleyDB r
    end


  fun dbDel' (db, dbtxn, key, flags) =
     let
      val r = db_del (db, dbTxn dbtxn, key, String.size key, dbFlagsAnd flags)
    in
      if r = 0 then () else
      if isAbsent r then () else raise BerkeleyDB r
    end


  fun dbDelRecno' (db, dbtxn, key, flags) =
     let
      val r = db_del_recno (db, dbTxn dbtxn, key, dbFlagsAnd flags)
    in
      if r = 0 then () else
      if isAbsent r then () else raise BerkeleyDB r
    end


  structure Hash =
  struct
    open Hash

    fun dbOpen   (dbtxn, filename, dbname, flags, mode) = Hash (dbOpen' (dbtxn, filename, dbname, HASH, flags, mode))
    fun dbClose  (Hash db, flags)                   = dbClose' (db, flags)
    fun dbPut    (Hash db, dbtxn, key, data, flags) = dbPut' (db, dbtxn, key, data, flags)
    fun dbGet    (Hash db, dbtxn, key, flags)       = dbGet' (db, dbtxn, key, flags)
    fun dbExists (Hash db, dbtxn, key, flags)       = dbExists' (db, dbtxn, key, flags)
    fun dbDel    (Hash db, dbtxn, key, flags)       = dbDel' (db, dbtxn, key, flags)
  end

  structure Recno =
  struct
    open Recno

    fun dbOpen   (dbtxn, filename, dbname, flags, mode) = Recno (dbOpen' (dbtxn, filename, dbname, RECNO, flags, mode))
    fun dbClose  (Recno db, flags)                   = dbClose' (db, flags)
    fun dbPut    (Recno db, dbtxn, key, data, flags) = dbPutRecno' (db, dbtxn, key, data, flags)
    fun dbGet    (Recno db, dbtxn, key, flags)       = dbGetRecno' (db, dbtxn, key, flags)
    fun dbExists (Recno db, dbtxn, key, flags)       = dbExistsRecno' (db, dbtxn, key, flags)
    fun dbDel    (Recno db, dbtxn, key, flags)       = dbDelRecno' (db, dbtxn, key, flags)
  end

end
