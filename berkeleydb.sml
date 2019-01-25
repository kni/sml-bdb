structure BerkeleyDB :
sig
  exception BerkeleyDB of int
  datatype dbType = BTREE | HASH | RECNO | QUEUE | UNKNOWN
  datatype dbFlag = DB_CREATE | DB_RECOVER | DB_APPEND
  type db
  type dbTxn
  val dbOpen   : dbTxn option * string option * string option * dbType * dbFlag list * int -> db
  val dbClose  : db * dbFlag list -> unit

  val dbPut    : db * dbTxn option * string * string * dbFlag list -> unit
  val dbGet    : db * dbTxn option * string * dbFlag list -> string option
  val dbExists : db * dbTxn option * string * dbFlag list -> bool
  val dbDel    : db * dbTxn option * string * dbFlag list -> unit

  val dbPutRecno    : db * dbTxn option * int * string * dbFlag list -> int
  val dbGetRecno    : db * dbTxn option * int * dbFlag list -> string option
  val dbExistsRecno : db * dbTxn option * int * dbFlag list -> bool
  val dbDelRecno    : db * dbTxn option * int * dbFlag list -> unit

end
=
struct
  open BerkeleyDB

  fun dbTxn NONE = NONE
    | dbTxn (SOME (DB_TXN dbtxn)) = SOME dbtxn

  datatype dbType = BTREE | HASH | RECNO | QUEUE | UNKNOWN

  fun dbTypeToInt BTREE   = 1
    | dbTypeToInt HASH    = 2
    | dbTypeToInt RECNO   = 3
    | dbTypeToInt QUEUE   = 4
    | dbTypeToInt UNKNOWN = 5


  datatype dbFlag = DB_CREATE | DB_RECOVER | DB_APPEND

  fun dbFlagToWord DB_CREATE  = 0wx1
    | dbFlagToWord DB_RECOVER = 0wx2
    | dbFlagToWord DB_APPEND  = 0wx2

  fun dbFlagsAnd flags = Word.toInt (List.foldl (fn (v,a) => Word.orb (dbFlagToWord v, a)) 0w0 flags)


  fun dbOpen (dbtxn, filename, dbname, dbtype, flags, mode) =
    let
      val db = db_create ()
      val r = db_open (db, dbTxn dbtxn, filename, dbname, (dbTypeToInt dbtype), (dbFlagsAnd flags), mode)
    in
      if r = 0 then DB db else raise BerkeleyDB r
    end


  fun dbClose (DB db, flags) =
    let
      val r = db_close (db, (dbFlagsAnd flags))
    in
      if r = 0 then () else raise BerkeleyDB r
    end


  fun dbPut (DB db, dbtxn, key, data, flags) =
    let
      val r = db_put (db, dbTxn dbtxn, key, String.size key, data, String.size data, dbFlagsAnd flags)
    in
      if r = 0 then () else raise BerkeleyDB r
    end


  fun dbPutRecno (DB db, dbtxn, key, data, flags) = db_put_recno (db, dbTxn dbtxn, key, data, String.size data, dbFlagsAnd flags)


  fun dbGet (DB db, dbtxn, key, flags) = db_get (db, dbTxn dbtxn, key, String.size key, dbFlagsAnd flags)


  fun dbGetRecno (DB db, dbtxn, key, flags) = db_get_recno (db, dbTxn dbtxn, key, dbFlagsAnd flags)


  fun dbExists (DB db, dbtxn, key, flags) =
    let
      val r = db_exists (db, dbTxn dbtxn, key, String.size key, dbFlagsAnd flags)
    in
      if r = 0 then true else
      if isAbsent r then false else raise BerkeleyDB r
    end


  fun dbExistsRecno (DB db, dbtxn, key, flags) =
    let
      val r = db_exists_recno (db, dbTxn dbtxn, key, dbFlagsAnd flags)
    in
      if r = 0 then true else
      if isAbsent r then false else raise BerkeleyDB r
    end


  fun dbDel (DB db, dbtxn, key, flags) =
     let
      val r = db_del (db, dbTxn dbtxn, key, String.size key, dbFlagsAnd flags)
    in
      if r = 0 then () else
      if isAbsent r then () else raise BerkeleyDB r
    end


  fun dbDelRecno (DB db, dbtxn, key, flags) =
     let
      val r = db_del_recno (db, dbTxn dbtxn, key, dbFlagsAnd flags)
    in
      if r = 0 then () else
      if isAbsent r then () else raise BerkeleyDB r
    end

end
