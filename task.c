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
exit_nicely(PGconn *conn1, PGconn *conn2)
{
  if(conn1)
  {
    PQfinish(conn1);
  }

  if(conn2)
  {
    PQfinish(conn2);
  }
    exit(1);
}


void populateFoo(PGconn *conn)
{
  PGresult   *res;
  const char *errormessage;
  int         copyresult, i, rowsize;
  const int   n = 100;
  char       *row;

  /* Start COPY command to populate foo's source table*/
  res = PQexec(conn, "COPY source FROM STDIN (DELIMITER ',')");

  if (PQresultStatus(res) == PGRES_FATAL_ERROR)
  {
      fprintf(stderr, "COPY command failed: %s", PQerrorMessage(conn));
      PQclear(res);
      exit_nicely(conn, NULL);
  }

  /* PGRES_COMMAND_OK */

  for(i = 0; i < n; i++)
  {
    if(asprintf(&row, "%d,%d,%d\n", i,i%3,i%6) < 0)
      return;
    copyresult = PQputCopyData(conn,row, strlen(row));
    /*keep trying until buffer is empty */
    while(copyresult == 0){}
    if(copyresult <= 0)
    {
      fprintf(stderr, "COPY INTO source failed: %s\n", PQerrorMessage(conn));
      PQclear(res);
      exit_nicely(conn, NULL);
    }
  }

  copyresult = PQputCopyEnd(conn, errormessage);
  if (errormessage)
  {
      fprintf(stderr, "COPY END failed: %s\n", errormessage);
      PQclear(res);
      exit_nicely(conn, NULL);
  }
  return;
}

void copyFromFootoBar(PGconn *fooconn, PGconn *barconn)
{
  PGresult   *foores, *barres;
  const char *errormessage;
  int foocopyresult = 0;
  int barcopyresult;
  char *foorow;

  /* Start COPY FROM source to STDOUT and
     COPY dest from STDIN */
  foores = PQexec(fooconn, "COPY source to STDOUT");

  if (PQresultStatus(foores) == PGRES_FATAL_ERROR)
  {
      fprintf(stderr, "COPY command failed: %s", PQerrorMessage(fooconn));
      PQclear(foores);
      exit_nicely(fooconn, barconn);
  }

  barres = PQexec(barconn, "COPY dest from STDIN");
  if (PQresultStatus(barres) == PGRES_FATAL_ERROR)
  {
      fprintf(stderr, "COPY command failed: %s", PQerrorMessage(barconn));
      PQclear(barres);
      exit_nicely(fooconn, barconn);
  }

  while(foocopyresult >= 0)
  {
    foocopyresult = PQgetCopyData(fooconn, &foorow, 0);
    /* fatal result*/
    if(foocopyresult == -2 ){
      fprintf(stderr, "COPY source failed: %s", PQerrorMessage(fooconn));
      PQclear(foores);
      exit_nicely(fooconn, barconn);
    }
    barcopyresult = PQputCopyData(barconn, foorow, foocopyresult);
    while(barcopyresult == 0){}
    if(barcopyresult <= 0)
    {
      fprintf(stderr, "COPY INTO source failed: %s\n", PQerrorMessage(barconn));
      PQclear(barres);
      exit_nicely(fooconn, barconn);
    }
    PQfreemem(foorow);
  }

  barcopyresult = PQputCopyEnd(barconn, errormessage);
  if (errormessage)
  {
      fprintf(stderr, "COPY END failed: %s\n", errormessage);
      PQclear(barres);
      exit_nicely(fooconn, barconn);
  }
  return;
}

int main(int argc, char **argv)
{
    const char *fooconninfo, *barconninfo;
    PGconn     *fooconn, *barconn;

    fooconninfo = "dbname = foo";
    barconninfo = "dbname = bar";

    /* establish connections to foo and bar */
    fooconn = PQconnectdb(fooconninfo);
    barconn = PQconnectdb(barconninfo);
    /* Check to see that the backend connection was successfully made */
    if (PQstatus(fooconn) != CONNECTION_OK || PQstatus(fooconn) != CONNECTION_OK)
    {
        fprintf(stderr, "Connection to database failed: %s\n", PQerrorMessage(fooconn));
        fprintf(stderr, "Connection to database failed: %s\n", PQerrorMessage(barconn));
        exit_nicely(fooconn, barconn);
    }

    PQflush(fooconn);
    PQflush(barconn);
    populateFoo(fooconn);
    copyFromFootoBar(fooconn, barconn);

    /* close the connections to the database and cleanup */
    PQfinish(fooconn);
    PQfinish(barconn);

    return 0;
}
