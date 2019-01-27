structure BerkeleyDB =
struct

local
  open MLton.Pointer
in
  exception BerkeleyDB of int

  datatype dbTxn = DB_TXN of t


  val db_create_ffi = _import "db_create": t ref * t * Word32.word -> int;
  fun db_create txnid =
    let
      val db = ref null
      val r  = db_create_ffi (db, null, 0w0)
   in
     if r = 0
     then (!db, case txnid of SOME p => p | NONE => null)
     else raise BerkeleyDB r
   end


  val db_set_re_len_ffi = _import "db_set_re_len": t * Word32.word -> int;
  fun db_set_re_len (db, re_len) =
    let
      val r = db_set_re_len_ffi (db, Word32.fromInt re_len)
    in
      if r = 0 then () else raise BerkeleyDB r
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
      val filename_p = stringOptionToPtr filename
      val dbname_p   = stringOptionToPtr dbname
      (* ToDo https://github.com/MLton/mlton/issues/53 *)
      val r = db_open_ffi (db, txnid, filename_p, dbname_p, dbtype, Word32.fromInt flags, mode)
    in
      freeNotNull filename_p;
      freeNotNull dbname_p;
      if r = 0 then () else raise BerkeleyDB r
    end


  val db_close_ffi = _import "db_close": t * Word32.word -> int;
  fun db_close (db, flags) =
    let
      val r = db_close_ffi (db, Word32.fromInt flags)
    in
      if r = 0 then () else raise BerkeleyDB r
    end


  val db_put_ffi = _import "db_put": t * t * string * Word32.word * string * Word32.word * Word32.word-> int;
  fun db_put (db, txnid, key, data, flags) =
    let
      val r = db_put_ffi (db, txnid, key, Word32.fromInt (String.size key), data, Word32.fromInt (String.size data), Word32.fromInt flags)
    in
      if r = 0 then () else raise BerkeleyDB r
    end

  val db_put_recno_ffi = _import "db_put_recno": t * t * Word32.word ref * string * Word32.word * Word32.word-> int;
  fun db_put_recno (db, txnid, key, data, flags) =
    let
      val key' = ref (Word32.fromInt key)
      val r = db_put_recno_ffi (db, txnid, key', data, Word32.fromInt (String.size data), Word32.fromInt flags)
    in
      if r = 0 then Word32.toInt (!key') else raise BerkeleyDB r
    end


  fun isAbsent r =
    if r = ~30988 orelse r = ~30994 orelse r = ~30995 (* DB_NOTFOUND, DB_KEYEXIST, DB_KEYEMPTY *)
    then true
    else false


  fun readMem (ptr:t, len) : string = Byte.bytesToString (Word8Array.vector (Word8Array.tabulate(Word32.toInt len, (fn(i) => getWord8(ptr, i)))))

  val db_get_ffi = _import "db_get": t * t * string * Word32.word * t ref * Word32.word ref * Word32.word-> int;
  fun db_get (db, txnid, key, flags) =
    let
      val data_r = ref null
      val data_len_r = ref 0w0
      val r  = db_get_ffi (db, txnid, key, Word32.fromInt (String.size key), data_r, data_len_r, Word32.fromInt flags)
    in
      if r = 0
      then SOME (readMem (!data_r, !data_len_r))
      else if isAbsent r then NONE else raise BerkeleyDB r
    end

  val db_get_recno_ffi = _import "db_get_recno": t * t * Word32.word * t ref * Word32.word ref * Word32.word-> int;
  fun db_get_recno (db, txnid, key, flags) =
    let
      val data_r = ref null
      val data_len_r = ref 0w0
      val r  = db_get_recno_ffi (db, txnid, Word32.fromInt key, data_r, data_len_r, Word32.fromInt flags)
    in
      if r = 0
      then SOME (readMem (!data_r, !data_len_r))
      else if isAbsent r then NONE else raise BerkeleyDB r
    end


  val db_exists_ffi = _import "db_exists": t * t * string * Word32.word * Word32.word-> int;
  fun db_exists (db, txnid, key, flags) =
    let
      val r = db_exists_ffi (db, txnid, key, Word32.fromInt (String.size key), Word32.fromInt flags)
    in
      if r = 0 then true else
      if isAbsent r then false else raise BerkeleyDB r
    end

  val db_exists_recno_ffi = _import "db_exists_recno": t * t * Word32.word * Word32.word-> int;
  fun db_exists_recno (db, txnid, key, flags) =
    let
      val r = db_exists_recno_ffi (db, txnid, Word32.fromInt key, Word32.fromInt flags)
    in
      if r = 0 then true else
      if isAbsent r then false else raise BerkeleyDB r
    end


  val db_del_ffi = _import "db_del": t * t * string * Word32.word * Word32.word-> int;
  fun db_del (db, txnid, key, flags) =
    let
      val r = db_del_ffi (db, txnid, key, Word32.fromInt (String.size key), Word32.fromInt flags)
    in
      if r = 0 then () else
      if isAbsent r then () else raise BerkeleyDB r
    end

  val db_del_recno_ffi = _import "db_del_recno": t * t * Word32.word * Word32.word-> int;
  fun db_del_recno (db, txnid, key, flags) =
    let
      val r = db_del_recno_ffi (db, txnid, Word32.fromInt key, Word32.fromInt flags)
    in
      if r = 0 then () else
      if isAbsent r then () else raise BerkeleyDB r
    end



  val db_cursor_ffi = _import "db_cursor": t * t * t ref * Word32.word -> int;
  fun db_cursor (db, txnid, flags) =
    let
      val dbc = ref null
      val r  = db_cursor_ffi (db, txnid, dbc, Word32.fromInt flags)
   in
     if r = 0
     then !dbc
     else raise BerkeleyDB r
   end


  val dbc_close_ffi = _import "dbc_close": t -> int;
  fun dbc_close dbc =
    let
      val r = dbc_close_ffi dbc
    in
      if r = 0 then () else raise BerkeleyDB r
    end


  val dbc_get_ffi = _import "dbc_get": t * t ref * Word32.word ref * t ref * Word32.word ref * Word32.word-> int;
  fun dbc_get (dbc, flags) =
    let
      val key_r = ref null
      val key_len_r = ref 0w0
      val data_r = ref null
      val data_len_r = ref 0w0
      val r  = dbc_get_ffi (dbc, key_r, key_len_r, data_r, data_len_r, Word32.fromInt flags)
    in
      if r = 0
      then SOME (readMem (!key_r, !key_len_r), readMem (!data_r, !data_len_r))
      else if isAbsent r then NONE else raise BerkeleyDB r
    end

  val dbc_get_recno_ffi = _import "dbc_get_recno": t * Word32.word ref * t ref * Word32.word ref * Word32.word-> int;
  fun dbc_get_recno (dbc, flags) =
    let
      val key_r = ref 0w0
      val data_r = ref null
      val data_len_r = ref 0w0
      val r  = dbc_get_recno_ffi (dbc, key_r, data_r, data_len_r, Word32.fromInt flags)
    in
      if r = 0
      then SOME (Word32.toInt (!key_r), readMem (!data_r, !data_len_r))
      else if isAbsent r then NONE else raise BerkeleyDB r
    end



  structure BTree =
  struct
    datatype db = BTree of t * t
    datatype dbc = BTreeCursor of t
  end

  structure Hash =
  struct
    datatype db = Hash of t * t
    datatype dbc = HashCursor of t
  end

  structure Recno =
  struct
    datatype db = Recno of t * t
    datatype dbc = RecnoCursor of t
  end

  structure Queue =
  struct
    datatype db = Queue of t * t
    datatype dbc = QueueCursor of t
  end
end

end
