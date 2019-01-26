structure BerkeleyDB =
struct

local
  open Foreign
  val lib = loadLibrary "db.so"
in
  exception BerkeleyDB of int

  datatype dbTxn = DB_TXN of Memory.voidStar


  val db_create_ffi = buildCall3 ((getSymbol lib "db_create"), (cStar cPointer, cOptionPtr cPointer, cUint32), cInt)
  fun db_create txnid =
    let
      val db = ref Memory.null
      val r  = db_create_ffi (db, NONE, 0)
   in
     if r = 0
     then (!db, case txnid of SOME p => p | NONE => Memory.null)
     else raise BerkeleyDB r
   end


  val db_set_re_len_ffi = buildCall2 ((getSymbol lib "db_set_re_len"), (cPointer, cUint32), cInt)
  fun db_set_re_len (db, re_len) =
    let
      val r = db_set_re_len_ffi (db, re_len)
    in
      if r = 0 then () else raise BerkeleyDB r
    end


  val db_open_ffi = buildCall7 ((getSymbol lib "db_open"), (cPointer, cPointer, cOptionPtr cString, cOptionPtr cString, cInt, cUint32, cInt), cInt)
  fun db_open (db, txnid, filename, dbname, dbtype, flags, mode) =
    let
      val r = db_open_ffi (db, txnid, filename, dbname, dbtype, flags, mode)
    in
      if r = 0 then () else raise BerkeleyDB r
    end


  val db_close_ffi = buildCall2 ((getSymbol lib "db_close"), (cPointer, cUint32), cInt)
  fun db_close (db, flags) =
    let
      val r = db_close_ffi (db, flags)
    in
      if r = 0 then () else raise BerkeleyDB r
    end


  val db_put_ffi = buildCall7 ((getSymbol lib "db_put"), (cPointer, cPointer, cString, cUint32, cString, cUint32, cUint32), cInt)
  fun db_put (db, txnid, key, data, flags) =
    let
      val r = db_put_ffi (db, txnid, key, String.size key, data, String.size data, flags)
    in
      if r = 0 then () else raise BerkeleyDB r
    end

  val db_put_recno_ffi = buildCall6 ((getSymbol lib "db_put_recno"), (cPointer, cPointer, cStar cUint32, cString, cUint32, cUint32), cInt)
  fun db_put_recno (db, txnid, key, data, flags) =
    let
      val key' = ref key
      val r = db_put_recno_ffi (db, txnid, key', data, String.size data, flags)
    in
      if r = 0 then !key' else raise BerkeleyDB r
    end


  fun isAbsent r =
    if r = ~30988 orelse r = ~30994 orelse r = ~30995 (* DB_NOTFOUND, DB_KEYEXIST, DB_KEYEMPTY *)
    then true
    else false


  fun readMem (mem:Memory.voidStar, len:int) : string = CharVector.tabulate (len, fn i => Byte.byteToChar (Memory.get8 (mem, Word.fromInt i)))

  val db_get_ffi = buildCall7 ((getSymbol lib "db_get"), (cPointer, cPointer, cString, cUint32, cStar cPointer, cStar cUint32, cUint32), cInt)
  fun db_get (db, txnid, key, flags) =
    let
      val data_r = ref Memory.null
      val data_len_r = ref 0
      val r  = db_get_ffi (db, txnid, key, String.size key, data_r, data_len_r, flags)
    in
      if r = 0
      then SOME (readMem (!data_r, !data_len_r))
      else if isAbsent r then NONE else raise BerkeleyDB r
    end

  val db_get_recno_ffi = buildCall6 ((getSymbol lib "db_get_recno"), (cPointer, cPointer, cUint32, cStar cPointer, cStar cUint32, cUint32), cInt)
  fun db_get_recno (db, txnid, key, flags) =
    let
      val data_r = ref Memory.null
      val data_len_r = ref 0
      val r  = db_get_recno_ffi (db, txnid, key, data_r, data_len_r, flags)
    in
      if r = 0
      then SOME (readMem (!data_r, !data_len_r))
      else if isAbsent r then NONE else raise BerkeleyDB r
    end


  val db_exists_ffi = buildCall5 ((getSymbol lib "db_exists"), (cPointer, cPointer, cString, cUint32, cUint32), cInt)
  fun db_exists (db, txnid, key, flags) =
    let
      val r = db_exists_ffi (db, txnid, key, String.size key, flags)
    in
      if r = 0 then true else
      if isAbsent r then false else raise BerkeleyDB r
    end

  val db_exists_recno_ffi = buildCall4 ((getSymbol lib "db_exists_recno"), (cPointer, cPointer, cUint32, cUint32), cInt)
  fun db_exists_recno (db, txnid, key, flags) =
    let
      val r = db_exists_recno_ffi (db, txnid, key, flags)
    in
      if r = 0 then true else
      if isAbsent r then false else raise BerkeleyDB r
    end


  val db_del_ffi = buildCall5 ((getSymbol lib "db_del"), (cPointer, cPointer, cString, cUint32, cUint32), cInt)
  fun db_del (db, txnid, key, flags) =
    let
      val r = db_del_ffi (db, txnid, key, String.size key, flags)
    in
      if r = 0 then () else
      if isAbsent r then () else raise BerkeleyDB r
    end

  val db_del_recno_ffi = buildCall4 ((getSymbol lib "db_del_recno"), (cPointer, cPointer, cUint32, cUint32), cInt)
  fun db_del_recno (db, txnid, key, flags) =
    let
      val r =  db_del_recno_ffi (db, txnid, key, flags)
    in
      if r = 0 then () else
      if isAbsent r then () else raise BerkeleyDB r
    end


  structure BTree =
  struct
    datatype db = BTree of Memory.voidStar * Memory.voidStar
  end

  structure Hash =
  struct
    datatype db = Hash of Memory.voidStar * Memory.voidStar
  end

  structure Recno =
  struct
    datatype db = Recno of Memory.voidStar * Memory.voidStar
  end

  structure Queue =
  struct
    datatype db = Queue of Memory.voidStar * Memory.voidStar
  end
end

end
