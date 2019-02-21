open BerkeleyDB

fun test_hash () =
  let
    val _ = print "Hash or BTree.\n"

    open Hash
    val dbtxn    = NONE
    val filename = NONE (* SOME "/tmp/foo.db" *)
    val dbname   = NONE
    val flags    = [DB_CREATE]
    val mode     = 0

    val db = dbCreate dbtxn
    val _ = dbOpen (db, filename, dbname, flags, mode)

    fun put key data = dbPut (db, key, data, [])

    val () = put "one"  "orange"
    val () = put "two"  "red"
    val () = put "tree" "yellow"
    val () = put "nine" "brown"
    val () = put "ten"  "black"

    val key  = "tree"
    val data = "yellow"

    val () = dbPut (db, key, data, [])
    val r = case dbGet (db, key, []) of
                NONE   => "ERROR"
              | SOME d => if d = data then "OK" else "ERROR"
    val _ = print ("Put and Get: " ^ r ^ "\n")


    val _ = if dbExists (db, key, [])
            then print ("Put and Exists: OK\n")
            else print ("Put and Exists: ERROR\n")


    val () = dbDel (db, key, [])
    val r = case dbGet (db, key, []) of
                NONE   => "OK"
              | SOME _ => "ERROR"
    val _ = print ("Del and Get: " ^ r ^ "\n")


    val _ = if dbExists (db, key, [])
            then print ("Del and Exists: ERROR\n")
            else print ("Del and Exists: OK\n")

    val _ = print "Cursor\n"
    val dbc = dbCursor (db, [])

    fun cursorLoop () =
      case dbcGet (dbc, [DB_NEXT]) of
          NONE => ()
        | SOME (key, data) => (print ("\t" ^ key ^ ": " ^ data ^ "\n"); cursorLoop ())

    val _ = cursorLoop ()
  in
    dbcClose dbc;
    dbClose (db, [])
  end


fun test_recno () =
  let
    val _ = print "Recno or Queue.\n"

    open Recno
    val dbtxn    = NONE
    val filename = NONE (* SOME "/tmp/foo.db" *)
    val dbname   = NONE
    val flags    = [DB_CREATE]
    val mode     = 0

    val db = dbCreate dbtxn
    (* val _ = dbSetReLen (db, 6) *) (* require for Queue *)

    val _ = dbOpen (db, filename, dbname, flags, mode)

    fun put key data = dbPut (db, key, data, [])

    val _ = put 1 "orange"
    val _ = put 2 "red"
    val _ = put 3 "yellow"
    val _ = put 9 "brown"

    val key = dbPut (db, 0, "black", [DB_APPEND])
    val _ = if key = 0 then print ("Put with DB_APPEND: ERROR\n") else print ("Put with DB_APPEND: OK\n")
    val _ = print ("DB_APPEND: " ^ Int.toString key ^ "\n")

    val key  = 3

    val r = case dbGet (db, key, []) of
                NONE   => "ERROR"
              | SOME d => if d = "yellow" then "OK" else ("ERROR (" ^ d ^ ")")
    val _ = print ("Put and Get: " ^ r ^ "\n")

    val () = dbDel (db, key, [])
    val r = case dbGet (db, key, []) of
                NONE   => "OK"
              | SOME _ => "ERROR"
    val _ = print ("Del and Get: " ^ r ^ "\n")

    val _ = if dbExists (db, key, [])
            then print ("Del and Exists: ERROR\n")
            else print ("Del and Exists: OK\n")

    val _ = print "Cursor\n"
    val dbc = dbCursor (db, [])

    fun cursorLoop () =
      case dbcGet (dbc, [DB_NEXT]) of
          NONE => ()
        | SOME (key, data) => (print ("\t" ^ Int.toString key ^ ": " ^ data ^ "\n"); cursorLoop ())

    val _ = cursorLoop ()
  in
    dbcClose dbc;
    dbClose (db, [])
  end


fun test_verify () =
  let
    val _ = print "Verify.\n"

    val file = "/tmp/too.db"
    val _ = OS.FileSys.remove file

    open Hash
    val dbtxn    = NONE
    val filename = SOME file
    val dbname   = NONE
    val flags    = [DB_CREATE]
    val mode     = 0

    val db = dbCreate dbtxn
    val () = dbOpen (db, filename, dbname, flags, mode)
    fun put key data = dbPut (db, key, data, [])
    val () = put "one"  "orange"
    val () = put "two"  "red"
    val () = put "tree" "yellow"
    val () = dbClose (db, [])

    val _ = if dbVerify (dbCreate dbtxn, file, dbname, []) then print "Verify: OK\n" else print "Verify: ERROR\n"

  in () end

fun main () = (
    test_hash ();
    test_recno ();
    (* test_verify (); *)
    print "The End.\n"
  ) handle exc => print ("function main raised an exception: " ^ exnMessage exc ^ "\n")
