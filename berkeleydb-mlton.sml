structure BerkeleyDB =
struct

local
  open MLton.Pointer
in
  exception BerkeleyDB of int

  datatype db    = DB of t
  datatype dbTxn = DB_TXN of t


  val db_create_ffi = _import "db_create": t ref * t * Word32.word -> int;
  fun db_create () =
    let
      val db = ref null
      val r  = db_create_ffi (db, null, 0w0)
   in
     if r = 0
     then !db
     else raise BerkeleyDB r
   end


  val malloc = (_import "malloc" : Word.word -> t;) o Word.fromInt

  val free = _import "free" : t -> unit;
  fun freeNotNull p = if p = null then () else free p;

  fun stringOptionToPtr NONE = null
    | stringOptionToPtr (SOME s) =
    let
      val p = malloc (String.size s)
    in
      CharVector.appi (fn (i, c) => setWord8 (p, i, Word8.fromInt(Char.ord c))) s;
      p
    end

  val db_open_ffi = _import "db_open": t * t * t * t * int * Word32.word * int-> int;
  fun db_open (db, txnid, filename, dbname, dbtype, flags, mode) =
    let
      val txnid = case txnid of NONE => null | SOME p => p
      val filename_p = stringOptionToPtr filename
      val dbname_p   = stringOptionToPtr dbname
      (* ToDo https://github.com/MLton/mlton/issues/53 *)
      val r = db_open_ffi (db, txnid, filename_p, dbname_p, dbtype, Word32.fromInt flags, mode)
    in
      freeNotNull filename_p;
      freeNotNull dbname_p;
      r
    end


  val db_close_ffi = _import "db_close": t * Word32.word -> int;
  fun db_close (db, flags) = db_close_ffi (db, Word32.fromInt flags)


  val db_put_ffi = _import "db_put": t * t * string * Word32.word * string * Word32.word * Word32.word-> int;
  fun db_put (db, txnid, key, key_len, data, data_len, flags) =
    let
      val txnid = case txnid of NONE => null | SOME p => p
    in
      db_put_ffi (db, txnid, key, Word32.fromInt key_len, data, Word32.fromInt data_len, Word32.fromInt flags)
    end

  val db_put_recno_ffi = _import "db_put_recno": t * t * Word32.word ref * string * Word32.word * Word32.word-> int;

  fun db_put_recno (db, txnid, key, data, data_len, flags) =
    let
      val txnid = case txnid of NONE => null | SOME p => p
      val key' = ref (Word32.fromInt key)
      val r = db_put_recno_ffi (db, txnid, key', data, Word32.fromInt data_len, Word32.fromInt flags)
    in
      if r = 0 then Word32.toInt (!key') else raise BerkeleyDB r
    end


  fun isAbsent r =
    if r = ~30988 orelse r = ~30996 (* DB_NOTFOUND and DB_KEYEMPTY *)
    then true
    else false


  fun readMem (ptr:t, len) : string = Byte.bytesToString (Word8Array.vector (Word8Array.tabulate(Word32.toInt len, (fn(i) => getWord8(ptr, i)))))

  val db_get_ffi = _import "db_get": t * t * string * Word32.word * t ref * Word32.word ref * Word32.word-> int;
  fun db_get (db, txnid, key, key_len, flags) =
    let
      val txnid = case txnid of NONE => null | SOME p => p
      val data_r = ref null
      val data_len_r = ref 0w0
      val r  = db_get_ffi (db, txnid, key, Word32.fromInt key_len, data_r, data_len_r, Word32.fromInt flags)
    in
      if r = 0
      then SOME (readMem (!data_r, !data_len_r))
      else if isAbsent r then NONE else raise BerkeleyDB r
    end

  val db_get_recno_ffi = _import "db_get_recno": t * t * Word32.word * t ref * Word32.word ref * Word32.word-> int;
  fun db_get_recno (db, txnid, key, flags) =
    let
      val txnid = case txnid of NONE => null | SOME p => p
      val data_r = ref null
      val data_len_r = ref 0w0
      val r  = db_get_recno_ffi (db, txnid, Word32.fromInt key, data_r, data_len_r, Word32.fromInt flags)
    in
      if r = 0
      then SOME (readMem (!data_r, !data_len_r))
      else if isAbsent r then NONE else raise BerkeleyDB r
    end

  val db_exists_ffi = _import "db_exists": t * t * string * Word32.word * Word32.word-> int;
  fun db_exists (db, txnid, key, key_len, flags) =
    let
      val txnid = case txnid of NONE => null | SOME p => p
    in
      db_exists_ffi (db, txnid, key, Word32.fromInt key_len, Word32.fromInt flags)
    end


  val db_del_ffi = _import "db_del": t * t * string * Word32.word * Word32.word-> int;
  fun db_del (db, txnid, key, key_len, flags) =
    let
      val txnid = case txnid of NONE => null | SOME p => p
    in
      db_del_ffi (db, txnid, key, Word32.fromInt key_len, Word32.fromInt flags)
    end
end

end
