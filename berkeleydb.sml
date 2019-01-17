(* use "berkeleydb-poly.sml"; *)
structure BerkeleyDB =
struct

local
  open Foreign
  val lib = loadLibrary "db.so"
in
  exception BerkeleyDB of int

  datatype db    = DB of Memory.voidStar
  datatype dbTxn = DB_TXN of Memory.voidStar


  val db_create_ffi = buildCall3 ((getSymbol lib "db_create"), (cStar cPointer, cOptionPtr cPointer, cUint32), cInt)
  fun db_create () =
    let
      val db = ref Memory.null
      val r  = db_create_ffi (db, NONE, 0)
   in
     if r = 0
     then !db
     else raise BerkeleyDB r
   end


  val db_open = buildCall7 ((getSymbol lib "db_open"), (cPointer, cOptionPtr cPointer, cOptionPtr cString, cOptionPtr cString, cInt, cUint32, cInt), cInt)

  val db_close = buildCall2 ((getSymbol lib "db_close"), (cPointer, cUint32), cInt)

  val db_put = buildCall7 ((getSymbol lib "db_put"), (cPointer, cOptionPtr cPointer, cString, cUint32, cString, cUint32, cUint32), cInt)


  fun isAbsent r =
    if r = ~30988 orelse r = ~30996 (* DB_NOTFOUND and DB_KEYEMPTY *)
    then true
    else false


  fun readMem (mem:Memory.voidStar, len:int) : string = CharVector.tabulate (len, fn i => Byte.byteToChar (Memory.get8 (mem, Word.fromInt i)))

  val db_get_ffi = buildCall7 ((getSymbol lib "db_get"), (cPointer, cOptionPtr cPointer, cString, cUint32, cStar cPointer, cStar cUint32, cUint32), cInt)
  fun db_get (db, dbtxn, key, key_len, flags) =
    let
      val data_r = ref Memory.null
      val data_len_r = ref 0
      val r  = db_get_ffi (db, dbtxn, key, key_len, data_r, data_len_r, flags)
    in
     if r = 0
     then SOME (readMem (!data_r, !data_len_r))
     else if isAbsent r then NONE else raise BerkeleyDB r
    end


  val db_exists = buildCall5 ((getSymbol lib "db_exists"), (cPointer, cOptionPtr cPointer, cString, cUint32, cUint32), cInt)

  val db_del = buildCall5 ((getSymbol lib "db_del"), (cPointer, cOptionPtr cPointer, cString, cUint32, cUint32), cInt)
end

end

(* ToDo Put above into berkeleydb-poly.sml *)


structure BerkeleyDB :
sig
  exception BerkeleyDB of int
  datatype dbType = BTREE | HASH | RECNO | QUEUE | UNKNOWN
  datatype dbFlag = DB_CREATE | DB_RECOVER
  type db
  type dbTxn
  val dbOpen   : dbTxn option * string option * string option * dbType * dbFlag list * int -> db
  val dbClose  : db * dbFlag list -> unit
  val dbPut    : db * dbTxn option * string * string * dbFlag list -> unit
  val dbGet    : db * dbTxn option * string * dbFlag list -> string option
  val dbExists : db * dbTxn option * string * dbFlag list -> bool
  val dbDel    : db * dbTxn option * string * dbFlag list -> unit
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


  datatype dbFlag = DB_CREATE | DB_RECOVER

  fun dbFlagToWord DB_CREATE  = 0wx1
    | dbFlagToWord DB_RECOVER = 0wx2

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


  fun dbGet (DB db, dbtxn, key, flags) = db_get (db, dbTxn dbtxn, key, String.size key, dbFlagsAnd flags)


  fun dbExists (DB db, dbtxn, key, flags) =
    let
      val r = db_exists (db, dbTxn dbtxn, key, String.size key, dbFlagsAnd flags)
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


end


open BerkeleyDB

fun test () =
  let
    val dbtxn    = NONE
    val filename = NONE (* SOME "/tmp/foo.db" *)
    val dbname   = NONE
    val dbtype   = HASH
    val flags    = [DB_CREATE]
    val mode     = 0
    val db = dbOpen (dbtxn, filename, dbname, dbtype, flags, mode)

    val key  = "my_key"
    val data = "my_data"


    val () = dbPut (db, dbtxn, key, data, [])
    val r = case dbGet (db, dbtxn, key, []) of
                NONE   => "ERROR"
              | SOME d => if d = data then "OK" else "ERROR"
    val _ = print ("Put and Get: " ^ r ^ "\n")


    val _ = if dbExists (db, dbtxn, key, [])
            then print ("Put and Exists: OK\n")
            else print ("Put and Exists: ERROR\n")


    val () = dbDel (db, dbtxn, key, [])
    val r = case dbGet (db, dbtxn, key, []) of
                NONE   => "OK"
              | SOME _ => "ERROR"
    val _ = print ("Del and Get: " ^ r ^ "\n")


    val _ = if dbExists (db, dbtxn, key, [])
            then print ("Del and Exists: ERROR\n")
            else print ("Del and Exists: OK\n")
  in
    dbClose (db, []);
    print "The End\n"
  end


val _ = test ()
