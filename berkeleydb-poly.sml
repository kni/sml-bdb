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
