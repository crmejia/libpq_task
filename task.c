/*
 * testlibpq.c
 *
 *      Test the C version of libpq, the PostgreSQL frontend library.
 */
#include <stdio.h>
#include <stdlib.h>
#include <libpq-fe.h>
#include <string.h>

static void
exit_nicely(PGconn *conn)
{
    PQfinish(conn);
    exit(1);
}

int
main(int argc, char **argv)
{
    const char *fooconninfo;
    PGconn     *conn;
    PGresult   *res;
    const char *errormessage;
    int         copyresult;
    fooconninfo = "dbname = foo";

    /* Make a connection to the database */
    conn = PQconnectdb(fooconninfo);

    /* Check to see that the backend connection was successfully made */
    if (PQstatus(conn) != CONNECTION_OK)
    {
        fprintf(stderr, "Connection to database failed: %s",
                PQerrorMessage(conn));
        exit_nicely(conn);
    }

    /* Start COPY command to populate foo's source table*/
    res = PQexec(conn, "COPY source FROM STDIN (DELIMITER ',')");

    if (PQresultStatus(res) == PGRES_FATAL_ERROR)
    {
        fprintf(stderr, "COPY command failed: %s", PQerrorMessage(conn));
        PQclear(res);
        exit_nicely(conn);
    }
    /* PGRES_COMMAND_OK */

    // copyresult = PQputCopyData(conn,"1,4,4\n4,1,4\n4,4,1\n", 18);
    copyresult = PQputCopyData(conn,"1,4,4\n", 6);
    copyresult = PQputCopyData(conn,"4,1,4\n", 6);
    copyresult = PQputCopyData(conn,"4,4,1\n", 6);

    if(copyresult <= 0)
    {
      fprintf(stderr, "COPY INTO source failed: %s", PQerrorMessage(conn));
      PQclear(res);
      exit_nicely(conn);
    }

    copyresult = PQputCopyEnd(conn, errormessage);
    if (errormessage)
    {
        fprintf(stderr, "COPY END failed: %s\n", errormessage);
        PQclear(res);
        exit_nicely(conn);
    }

    /* close the connection to the database and cleanup */
    PQfinish(conn);

    return 0;
}
