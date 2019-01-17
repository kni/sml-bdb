(* use "berkeleydb-poly.sml"; *)
structure BerkeleyDB =
struct

local
  open Foreign
  val lib = loadLibrary "db.so"
in
  exception BerkeleyDB of int

  type db    = Memory.voidStar
  type dbTxn = Memory.voidStar


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


  val db_open = buildCall7 ((getSymbol lib "db_open"), (cPointer, cOptionPtr cPointer, cString, cOptionPtr cString, cInt, cUint32, cInt), cInt)

  val db_close = buildCall2 ((getSymbol lib "db_close"), (cPointer, cUint32), cInt)
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
  val dbOpen  : dbTxn option * string * string option * dbType * dbFlag list * int -> db
  val dbClose : db * dbFlag list -> unit
end
=
struct
  open BerkeleyDB

  datatype dbType = BTREE | HASH | RECNO | QUEUE | UNKNOWN

  fun dbTypeToInt BTREE   = 1
    | dbTypeToInt HASH    = 2
    | dbTypeToInt RECNO   = 3
    | dbTypeToInt QUEUE   = 4
    | dbTypeToInt UNKNOWN = 5


  datatype dbFlag = DB_CREATE | DB_RECOVER

  fun dbFlagToWord DB_CREATE  = 0wx1
    | dbFlagToWord DB_RECOVER = 0wx2

  fun dbFlagsAnd flags = Word.toInt (List.foldl (fn (v,a) => Word.andb (dbFlagToWord v, a)) 0w0 flags)


  fun dbOpen (db_txn, filename, dbname, db_type, flags, mode) =
    let
      val db = db_create ()
      val r = db_open (db, db_txn, filename, dbname, (dbTypeToInt db_type), (dbFlagsAnd flags), mode)
    in
      if r = 0
      then db
      else raise BerkeleyDB r
    end


  fun dbClose (db, flags) =
    let
      val r = db_close (db, (dbFlagsAnd flags))
    in
      if r = 0
      then ()
      else raise BerkeleyDB r
    end


fun test () =
  let
    val db = dbOpen (NONE, "/tmp/foo.db", NONE, HASH, [DB_CREATE], 0)
  in
    dbClose (db, []);
    print "The End\n"
  end


val _ = test ()

end
