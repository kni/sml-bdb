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


fun main () = test () handle exc => print ("function main raised an exception: " ^ exnMessage exc ^ "\n")
