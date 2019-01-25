open BerkeleyDB

fun test_hash () =
  let
    open Hash
    val dbtxn    = NONE
    val filename = NONE (* SOME "/tmp/foo.db" *)
    val dbname   = NONE
    val flags    = [DB_CREATE]
    val mode     = 0
    val db = dbOpen (dbtxn, filename, dbname, flags, mode)

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
    dbClose (db, [])
  end


fun test_recno () =
  let
    open Recno
    val dbtxn    = NONE
    val filename = NONE (* SOME "/tmp/foo.db" *)
    val dbname   = NONE
    val flags    = [DB_CREATE]
    val mode     = 0
    val db = dbOpen (dbtxn, filename, dbname, flags, mode)

    val _ = print "Recno\n"

    fun put key data = dbPut (db, dbtxn, key, data, [])
    val _ = put 1 "orange"
    val _ = put 2 "red"
    val _ = put 3 "yellow"
    val _ = put 9 "brown"

    val key = dbPut (db, dbtxn, 0, "black", [DB_APPEND])
    val _ = if key = 0 then print ("Put with DB_APPEND: ERROR\n") else print ("Put with DB_APPEND: OK\n")
    val _ = print ("DB_APPEND: " ^ Int.toString key ^ "\n")

    val key  = 3

    val r = case dbGet (db, dbtxn, key, []) of
                NONE   => "ERROR"
              | SOME d => if d = "yellow" then "OK" else "ERROR"
    val _ = print ("Put and Get: " ^ r ^ "\n")

    val () = dbDel (db, dbtxn, key, [])
    val r = case dbGet (db, dbtxn, key, []) of
                NONE   => "OK"
              | SOME _ => "ERROR"
    val _ = print ("Del and Get: " ^ r ^ "\n")

    val _ = if dbExists (db, dbtxn, key, [])
            then print ("Del and Exists: ERROR\n")
            else print ("Del and Exists: OK\n")
  in
    dbClose (db, [])
  end

fun main () = (test_hash () ; test_recno () ; print "The End\n" ) handle exc => print ("function main raised an exception: " ^ exnMessage exc ^ "\n")
