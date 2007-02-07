/*--------------------------------------------------------------------
** Teradata::SQL
** C routines with CLI calls
** These routines, like many Perl routines, return 1 for success
** and 0 for failure.
**------------------------------------------------------------------*/
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include "tdsql.h"

static Int32  result;
static char   cnta[4];

/*--------------------------------------------------------------------
** Global variables from SQL.xs
**------------------------------------------------------------------*/

extern int g_msglevel;
extern double g_activcount;
extern int g_errorcode;
extern char g_errormsg[260];

/*--------------------------------------------------------------------
** Powers of 10
**------------------------------------------------------------------*/

static double powers10[19] = {
  1.0, 10.0, 100.0, 1000.0, 10000.0,
  1.0E5,  1.0E6,  1.0E7,  1.0E8,  1.0E9,  1.0E10, 1.0E11,
  1.0E12, 1.0E13, 1.0E14, 1.0E15, 1.0E16, 1.0E17, 1.0E18 };


/*--------------------------------------------------------------------
** Error checking routine for CLI calls. Return 0 if there was an
** error, 1 otherwise.
**------------------------------------------------------------------*/
int check_cli_error (
  const char * sql_command,
  struct DBCAREA * dbc )
{
 if (result == EM_OK) {
    return 1;
 } else {
    g_errorcode = result;
    strcpy(g_errormsg, dbc->msg_text);
    if (g_msglevel >= 1) {
       fprintf(stderr, "Error in %s\n", sql_command);
       fprintf(stderr, "%s\n", dbc->msg_text);
    }
    return 0;
 }
}

/*--------------------------------------------------------------------
**  Convert data types from a PrepInfo parcel to simplified types.
**------------------------------------------------------------------*/
void _simplify_prepinfo (
  struct datadescr * desc_ptr,
  char * parcel )
{
 int   i;
 int   nftr;  /* Number of fields to return */
 struct CliPrepInfoType * prep_ptr;
 char * col_byte_ptr;
 struct CliPrepColInfoType * col_info;

 prep_ptr = (struct CliPrepInfoType *) parcel;
 col_byte_ptr = parcel + sizeof(struct CliPrepInfoType);
 col_info = (struct CliPrepColInfoType *) col_byte_ptr;

  /* We don't support WITH, so we ignore the SummaryCount. */
 if (prep_ptr->ColumnCount <= MAX_FIELDS) {
    nftr = desc_ptr->nfields = prep_ptr->ColumnCount;
 } else {
    nftr = desc_ptr->nfields = MAX_FIELDS;
    fprintf(stderr, "Only the first %d fields will be processed\n",
       MAX_FIELDS);
 }

 for (i = 0; i < nftr; i++) {
    switch (col_info->DataType) {
     case SMALLINT_NN:
     case SMALLINT_N:
        desc_ptr->sqlvar[i].sqltype = SMALLINT_N;
        desc_ptr->sqlvar[i].dlb = sizeof(short);
        break;
     case VARCHAR_NN:
     case VARCHAR_N:
     case VARBYTE_NN:
     case VARBYTE_N:
     case VARGRAPHIC_NN:
     case VARGRAPHIC_N:
     case LONG_VARCHAR_NN:
     case LONG_VARCHAR_N:
     case LONG_VARBYTE_NN:
     case LONG_VARBYTE_N:
     case LONG_VARGRAPHIC_NN:
     case LONG_VARGRAPHIC_N:
        desc_ptr->sqlvar[i].sqltype = VARCHAR_N;
        desc_ptr->sqlvar[i].datalen = col_info->DataLen;
        break;
     case CHAR_NN:
     case CHAR_N:
     case BYTE_NN:
     case BYTE_N:
     case GRAPHIC_NN:
     case GRAPHIC_N:
        desc_ptr->sqlvar[i].sqltype = CHAR_N;
        desc_ptr->sqlvar[i].datalen = col_info->DataLen;
        break;
     case DATE_NN:
     case DATE_N:
     case INTEGER_NN:
     case INTEGER_N:
        desc_ptr->sqlvar[i].sqltype = INTEGER_N;
        desc_ptr->sqlvar[i].dlb = sizeof(Int32);
        break;
     case DECIMAL_NN:
     case DECIMAL_N:
        desc_ptr->sqlvar[i].sqltype = DECIMAL_N;
        desc_ptr->sqlvar[i].datalen  = col_info->DataLen / 256;
        if (desc_ptr->sqlvar[i].datalen >= 10)
           desc_ptr->sqlvar[i].dlb = 8;
        else if (desc_ptr->sqlvar[i].datalen >= 5)
           desc_ptr->sqlvar[i].dlb = 4;
        else if (desc_ptr->sqlvar[i].datalen >= 3)
           desc_ptr->sqlvar[i].dlb = 2;
        else
           desc_ptr->sqlvar[i].dlb = 1;
        desc_ptr->sqlvar[i].decscale = col_info->DataLen % 256;
        break;
     case BLOB:
     case BLOB_DEFERRED:
     case BLOB_LOCATOR:
     case CLOB:
     case CLOB_DEFERRED:
     case CLOB_LOCATOR:
        desc_ptr->sqlvar[i].sqltype = BLOB;
        desc_ptr->sqlvar[i].datalen = col_info->DataLen;
        break;
     case BYTEINT_NN:
     case BYTEINT_N:
        desc_ptr->sqlvar[i].sqltype = BYTEINT_N;
        desc_ptr->sqlvar[i].dlb = 1;
        break;
     case FLOAT_NN:
     case FLOAT_N:
        desc_ptr->sqlvar[i].sqltype = FLOAT_N;
        desc_ptr->sqlvar[i].dlb = sizeof(double);
        break;
     default:
        desc_ptr->sqlvar[i].sqltype = -1;
        desc_ptr->sqlvar[i].datalen = 0;
    }

     /* Get the name of the column (not the title). */
    memcpy(desc_ptr->sqlvar[i].colident, col_info->Name, col_info->NameLen);
    desc_ptr->sqlvar[i].colident[col_info->NameLen] = '\0';

     /* Point to the next set of column info. This is a pain. */
    col_byte_ptr += 4;
    col_byte_ptr += *((short *)col_byte_ptr) + 2; /* Name */
    col_byte_ptr += *((short *)col_byte_ptr) + 2; /* Format */
    col_byte_ptr += *((short *)col_byte_ptr) + 2; /* Title */
    col_info = (struct CliPrepColInfoType *) col_byte_ptr;
 }
}

/*--------------------------------------------------------------------
 *  Insert a decimal point into a "decimal" field.
 *------------------------------------------------------------------*/
void _insert_dp (
 char *   target,
 char *   source,
 int      ndec )
{
 int   i, j, workstart;
 char  work[25];

 if (ndec <= 0) {
    strcpy(target, source);
    return;
 }

 work[24] = '\0';  /* End of string */

  /* Build the string in the work area from right to left. */

 i = strlen(source) - 1;
 for (j = 23; j > 23 - ndec; j--) {
    if (i < 0 || source[i] == '-' ) {
       work[j] = '0';
       continue;
    }
    /* Otherwise, it should be a digit. */
    work[j] = source[i];
    i--;
 }

 work[j] = '.';
 j--;

 for (; j >= 0; j--) {
    if (i < 0) {
       workstart = j+1;
       break;
    }
    if ( source[i] == '-' ) {
       work[j] = '-';
       workstart = j;
       break;
    }
    /* Otherwise, it should be a digit. */
    work[j] = source[i];
    i--;
 }

 strcpy(target, work+workstart);
 return;
}


/*--------------------------------------------------------------------
**  Convert a decimal field to a double.  This works only on
**  fields of 9 digits or less.
**------------------------------------------------------------------*/
double _dec_to_double (
  Byte * dec_data,
  int     decp,
  int     decs )
{
 long    wlong;
 double  wdouble;

 if (decp >= 5) {
    wlong = *((long *) dec_data);
 } else if (decp >= 3) {
    wlong = *((short *) dec_data) + 0;
 } else {		/* Precision is less than 3. */
    wlong = *((char *) dec_data) + 0;
 }

 wdouble = (double) wlong;
 if (decs > 0)
    wdouble /= powers10[decs];
 return wdouble;
}

/*--------------------------------------------------------------------
**  Convert a decimal field (10 or more digits) to a string.
**------------------------------------------------------------------*/
#ifdef _MSC_VER
 /*---------------------- Microsoft Visual C++ */
void _dec_to_string (
  char *  res_string,
  Byte * dec_data,
  int     decs )
{
 __int64  wlonglong;
 char   wstring[24];

 wlonglong = *((_int64 *) dec_data);
 sprintf(wstring, "%I64d", wlonglong);
 _insert_dp(res_string, wstring, decs);
}
#else
 /*---------------------- Others */
void _dec_to_string (
  char *  res_string,
  Byte * dec_data,
  int     decs )
{
 long long int  wlonglong;
 char   wstring[24];

 wlonglong = *((long long *) dec_data);
 sprintf(wstring, "%lld", wlonglong);
 _insert_dp(res_string, wstring, decs);
}
#endif

/*--------------------------------------------------------------------
**  Set CLI options.
**------------------------------------------------------------------*/
void set_options ( struct DBCAREA * dbcp )
{
  dbcp->change_opts = 'Y';
  dbcp->resp_mode = 'I';	  /* Indicator mode */
  dbcp->use_presence_bits = 'Y';  /* Indicator bits on USING parcels */
  dbcp->req_proc_opt = 'B';       /* Include Info parcels */
  dbcp->keep_resp = 'N';
  dbcp->wait_across_crash = 'N';
  dbcp->tell_about_crash = 'Y';
  dbcp->loc_mode = 'Y';
  dbcp->var_len_req = 'N';
  dbcp->var_len_fetch = 'N';
  dbcp->save_resp_buf = 'N';
  dbcp->two_resp_bufs = 'N';
  dbcp->ret_time = 'N';
  dbcp->parcel_mode = 'Y';
  dbcp->wait_for_resp = 'Y';

  dbcp->maximum_parcel = 'H';	/* Allow large parcels */
  dbcp->req_buf_len  = 65536;
  dbcp->resp_buf_len = 65473;

  return;
}

/*--------------------------------------------------------------------
**  Fetch a single parcel. Return 0 if a failure|error parcel was
**  found, 1 if a non-error, 2 if no parcels are left.
**------------------------------------------------------------------*/
int _fetch_parcel (
  const char * sql_command,
  struct DBCAREA * dbcp,
  pRequest req_ptr )
{
 int    status;
 struct CliSuccessType * pcl_success_ptr;
 unsigned long int  lcl_activcount;
 struct CliFailureType * err_ptr;

 if (req_ptr == 0) {
    dbcp->i_req_id  = dbcp->o_req_id;
 } else {
    dbcp->i_req_id = req_ptr->req_num;
 }

 dbcp->func = DBFFET;
 DBCHCL(&result,cnta, dbcp);

 if (result == REQEXHAUST)
    return(2);
 status = check_cli_error("CLI Fetch", dbcp);
 if (status == 0)
    return(0);

   /* Did Teradata issue an error message? */
 if (dbcp->fet_parcel_flavor == PclFAILURE ||
     dbcp->fet_parcel_flavor == PclERROR ) {
    err_ptr = (struct CliFailureType *) dbcp->fet_data_ptr;
    g_errorcode = err_ptr->Code;
    memcpy(g_errormsg, err_ptr->Msg, err_ptr->Length);
    g_errormsg[err_ptr->Length] = '\0';
    if (g_msglevel >= 1) {
       fprintf(stderr, "Error in %s\n", sql_command);
       fprintf(stderr, "  Error code: %d   Info: %d\n",
         err_ptr->Code, err_ptr->Info);
       fprintf(stderr, "  %s\n", g_errormsg);
    }
    return(0);
 }

   /* At this point, we have a valid non-error parcel. */
 switch (dbcp->fet_parcel_flavor) {
   case PclSUCCESS :
     g_errorcode = 0;
     strcpy(g_errormsg, "");
     pcl_success_ptr = (struct CliSuccessType *) dbcp->fet_data_ptr;
      /* Store the ActivityCount in a double. Unfortunately, this
         is defined as char[4] rather than unsigned int. */
     memcpy(&lcl_activcount, pcl_success_ptr->ActivityCount, 4);
     g_activcount = (double) lcl_activcount;
     break;
   case PclPREPINFO :
     if (req_ptr == 0) {
        fprintf(stderr, "Internal error in _fetch_parcel!\n");
        return 0;
     }
     _simplify_prepinfo(&(req_ptr->ddesc), dbcp->fet_data_ptr);
     break;
   default:
     ;
 } /* end switch */

 return(1);
}

/*--------------------------------------------------------------------
**  Fetch all parcels after a query
**------------------------------------------------------------------*/
int _fetch_all_parcels (
  const char * sql_command,
  struct DBCAREA * dbcp,
  pRequest req_ptr )
{
 int    status, normal, erq;

 status = 1;
 normal = 1;

 while (status != 2) {
    status = _fetch_parcel(sql_command, dbcp, req_ptr);
    if (status == 0) normal = 0;
 }

 dbcp->func = DBFERQ;
 DBCHCL(&result,cnta, dbcp);
 erq = check_cli_error("End Request", dbcp);

 if (normal == 0 || erq == 0)	return(0);
 else	return(1);
}

/*--------------------------------------------------------------------
**  CONNECT
**------------------------------------------------------------------*/
int Zconnect (
  pSession sess_ptr,
  char * logonstring,
  char * ccs,
  char * trx_mode )
{
 struct DBCAREA * dbcp;

 dbcp = &(sess_ptr->dbc);

 dbcp->total_len = sizeof(struct DBCAREA);
 DBCHINI(&result,cnta, dbcp);
 if (check_cli_error("CONNECT", dbcp) == 0) {
    return(0);
 }

 set_options(dbcp);
 if (strcmp(trx_mode, "ANSI") == 0) {
    dbcp->tx_semantics = 'A';
 } else {
    dbcp->tx_semantics = 'T';
 }
 sprintf(sess_ptr->ccs, "%-30s", ccs);
 dbcp->charset_type = 'N';
 dbcp->inter_ptr = sess_ptr->ccs;  /* Client character set */

 dbcp->logon_ptr = logonstring;
 dbcp->logon_len = strlen(logonstring);
 dbcp->func = DBFCON;

 DBCHCL(&result,cnta, dbcp);
 if (check_cli_error("CONNECT", dbcp) == 0)
    return 0;

 dbcp->i_sess_id = dbcp->o_sess_id;

 return _fetch_all_parcels("CONNECT", dbcp, 0);
}

/*--------------------------------------------------------------------
**  DISCONNECT (LOGOFF)
**------------------------------------------------------------------*/
int Zdisconnect (
  pSession sess )
{
 struct DBCAREA * dbcp;

 dbcp = &(sess->dbc);

 g_errorcode = 0;
 strcpy(g_errormsg, "");

 dbcp->change_opts = 'N';
 dbcp->func = DBFDSC;

 DBCHCL(&result,cnta, dbcp);
 if (check_cli_error("DISCONNECT", dbcp) == 0)
    return 0;

  /* There should be only one parcel (Success or Failure). */
 dbcp->func = DBFFET;
 DBCHCL(&result,cnta, dbcp);
 if (dbcp->fet_parcel_flavor == PclSUCCESS)
    return 1;
 else
    return 0;
}

/*--------------------------------------------------------------------
**  EXECUTE (no data returned)
**------------------------------------------------------------------*/
int Zexecute (
  pSession   sess,
  char * sql_stmt )
{
 struct DBCAREA * dbcp;

 dbcp = &(sess->dbc);

 dbcp->change_opts = 'Y';
 dbcp->req_proc_opt = 'E';	/* Execute only */
 dbcp->i_req_id = 0;
 dbcp->extension_pointer = 0;
 dbcp->req_ptr = sql_stmt;
 dbcp->req_len = strlen(sql_stmt);

 dbcp->func = DBFIRQ;
 DBCHCL(&result,cnta, dbcp);
 if (check_cli_error("EXECUTE", dbcp) == 0)	return(0);

 return _fetch_all_parcels("EXECUTE", dbcp, 0);
}

/*--------------------------------------------------------------------
**  OPEN a request.
**------------------------------------------------------------------*/
int Zopen (
  pRequest   req,
  char * sql_stmt )
{
 int   status;
 struct DBCAREA * dbcp;

 dbcp = req->dbcp;

 dbcp->change_opts = 'Y';
 dbcp->req_proc_opt = 'B';	/* Prepare and execute */
 dbcp->i_req_id = 0;
 dbcp->extension_pointer = 0;
 dbcp->req_ptr = sql_stmt;
 dbcp->req_len = strlen(sql_stmt);

 dbcp->func = DBFIRQ;
 DBCHCL(&result,cnta, dbcp);

 dbcp->i_req_id = dbcp->o_req_id;
 req->req_num = dbcp->o_req_id;

 if (check_cli_error("OPEN", dbcp) == 0)	return(0);

  /* Fetch only until we have the PrepInfo parcel. */
 status = 1;
 while (dbcp->fet_parcel_flavor != PclPREPINFO && status == 1) {
    status = _fetch_parcel("OPEN", dbcp, req);
 }

 return((status == 0) ? 0 : 1);
}

/*--------------------------------------------------------------------
**  EXECUTE a Prepared request
**------------------------------------------------------------------*/
int Zexecutep (
  pSession   sess,
  char * sql_stmt )
{
 struct DBCAREA * dbcp;

 dbcp = &(sess->dbc);

 dbcp->change_opts = 'Y';
 dbcp->req_proc_opt = 'E';	/* Execute only */
 dbcp->i_req_id = 0;
 dbcp->extension_pointer = 0;
 dbcp->req_ptr = sql_stmt;
 dbcp->req_len = strlen(sql_stmt);

 dbcp->func = DBFIRQ;
 DBCHCL(&result,cnta, dbcp);
 if (check_cli_error("EXECUTEP", dbcp) == 0)	return(0);

 dbcp->i_req_id = dbcp->o_req_id;

 return _fetch_all_parcels("EXECUTEP", dbcp, 0);
}

/*--------------------------------------------------------------------
**  EXECUTE a Prepared request with arguments
**------------------------------------------------------------------*/
int Zexecutep_args (
  pSession    sess,
  char * sql_stmt,
  struct ModCliDataInfoType * hv_datainfo,
  Byte * hv_data,
  int    datalen )
{
 int  Elen;
 struct irq_ext   ExtArea;
 struct irq_ext * pExtArea;
 struct DBCAREA * dbcp;

 dbcp = &(sess->dbc);

 dbcp->change_opts = 'Y';
 dbcp->req_proc_opt = 'E';	/* Execute only */
 dbcp->i_req_id = 0;
 dbcp->req_ptr = sql_stmt;
 dbcp->req_len = strlen(sql_stmt);

  /* Set up the Extension area. */
 dbcp->extension_pointer = &ExtArea;
 pExtArea = &ExtArea;
 Elen = sizeof( struct irq_ext );
 memset( pExtArea, 0x00, Elen);

 memcpy(pExtArea->irqx_header.d8xiId, "IRX8", 4);
 pExtArea->irqx_header.d8xiSize = Elen;
 pExtArea->irqx_header.d8xiLvl = 1;

  /* DataInfo parcel */
 pExtArea->irqx_DataInfo_elem.d8xieLen = sizeof(D8XIELEM) + sizeof(D8XIEP);
  /* The manual says that Element Type 0 = Inline and 1 = Pointer,
     but that seems to be incorrect. We are using pointers and 0. */
 pExtArea->irqx_DataInfo_elem.d8xieTyp = 0;  /* Pointer method */

 pExtArea->irqx_DataInfo_body.d8xiepF = PclDATAINFO; /* Flavor */
 pExtArea->irqx_DataInfo_body.d8xiepLn = sizeof(struct ModCliDataInfoType);
 pExtArea->irqx_DataInfo_body.d8xiepPt = (char *) hv_datainfo;

  /* IndicData parcel */
 pExtArea->irqx_IndicData_elem.d8xieLen = sizeof(D8XIELEM) + sizeof(D8XIEP);
 pExtArea->irqx_IndicData_elem.d8xieTyp = 0;
 pExtArea->irqx_IndicData_body.d8xiepF = PclINDICDATA; /* Flavor */
 pExtArea->irqx_IndicData_body.d8xiepLn = datalen;
 pExtArea->irqx_IndicData_body.d8xiepPt = (char *) hv_data;

 dbcp->func = DBFIRQ;
 DBCHCL(&result,cnta, dbcp);
 if (check_cli_error("EXECUTEP", dbcp) == 0)	return(0);

 dbcp->i_req_id = dbcp->o_req_id;

 return _fetch_all_parcels("EXECUTEP", dbcp, 0);
}

/*--------------------------------------------------------------------
**  OPEN a prepared request.
**------------------------------------------------------------------*/
int Zopenp (
  pRequest   req,
  char * sql_stmt )
{
 int   status;
 struct DBCAREA * dbcp;

 dbcp = req->dbcp;

 dbcp->change_opts = 'Y';
 dbcp->req_proc_opt = 'B';	/* Prepare and execute */
 dbcp->i_req_id = 0;
 dbcp->extension_pointer = 0;
 dbcp->req_ptr = sql_stmt;
 dbcp->req_len = strlen(sql_stmt);

 dbcp->func = DBFIRQ;
 DBCHCL(&result,cnta, dbcp);

 req->req_num = dbcp->o_req_id;
 dbcp->i_req_id = dbcp->o_req_id;

 if (check_cli_error("OPEN", dbcp) == 0)	return(0);

  /* Fetch only until we have the PrepInfo parcel. */
 status = 1;
 while (dbcp->fet_parcel_flavor != PclPREPINFO && status == 1) {
    status = _fetch_parcel("OPENP", dbcp, req);
 }

 return((status == 0) ? 0 : 1);
}

/*--------------------------------------------------------------------
**  OPEN a Prepared request with arguments
**------------------------------------------------------------------*/
int Zopenp_args (
  pRequest    req,
  char * sql_stmt,
  struct ModCliDataInfoType * hv_datainfo,
  Byte * hv_data,
  int    datalen )
{
 int status, Elen;
 struct irq_ext   ExtArea;
 struct irq_ext * pExtArea;
 struct DBCAREA * dbcp;

 dbcp = req->dbcp;

 dbcp->change_opts = 'Y';
 dbcp->req_proc_opt = 'B';	/* Prepare and execute */
 dbcp->i_req_id = 0;
 dbcp->req_ptr = sql_stmt;
 dbcp->req_len = strlen(sql_stmt);

  /* Set up the Extension area. */
 dbcp->extension_pointer = &ExtArea;
 pExtArea = &ExtArea;
 Elen = sizeof( struct irq_ext );
 memset( pExtArea, 0x00, Elen);

 memcpy(pExtArea->irqx_header.d8xiId, "IRX8", 4);
 pExtArea->irqx_header.d8xiSize = Elen;
 pExtArea->irqx_header.d8xiLvl = 1;

  /* DataInfo parcel */
 pExtArea->irqx_DataInfo_elem.d8xieLen = sizeof(D8XIELEM) + sizeof(D8XIEP);
  /* The manual says that Element Type 0 = Inline and 1 = Pointer,
     but that seems to be incorrect. We are using pointers and 0. */
 pExtArea->irqx_DataInfo_elem.d8xieTyp = 0;  /* Pointer method */

 pExtArea->irqx_DataInfo_body.d8xiepF = PclDATAINFO; /* Flavor */
 pExtArea->irqx_DataInfo_body.d8xiepLn = sizeof(struct ModCliDataInfoType);
 pExtArea->irqx_DataInfo_body.d8xiepPt = (char *) hv_datainfo;

  /* IndicData parcel */
 pExtArea->irqx_IndicData_elem.d8xieLen = sizeof(D8XIELEM) + sizeof(D8XIEP);
 pExtArea->irqx_IndicData_elem.d8xieTyp = 0;
 pExtArea->irqx_IndicData_body.d8xiepF = PclINDICDATA; /* Flavor */
 pExtArea->irqx_IndicData_body.d8xiepLn = datalen;
 pExtArea->irqx_IndicData_body.d8xiepPt = (char *) hv_data;

 dbcp->func = DBFIRQ;
 DBCHCL(&result,cnta, dbcp);
 if (check_cli_error("OPENP", dbcp) == 0)	return(0);

 req->req_num = dbcp->o_req_id;
 dbcp->i_req_id = dbcp->o_req_id;

  /* Fetch only until we have the PrepInfo parcel. */
 status = 1;
 while (dbcp->fet_parcel_flavor != PclPREPINFO && status == 1) {
    status = _fetch_parcel("OPENP", dbcp, req);
 }

 return((status == 0) ? 0 : 1);
}

/*--------------------------------------------------------------------
**  FETCH (one record)
**------------------------------------------------------------------*/
char * Zfetch (
  pRequest  req )
{
 int status;
 struct DBCAREA * dbcp;

 dbcp = req->dbcp;

 dbcp->change_opts = 'N';
 dbcp->i_req_id = req->req_num;

  /* Read the record. If we find a DataInfo parcel, we need to keep
     reading until we get a Record; if we find EndStatement, we keep
     reading until we get EndRequest. */

 status = 1;

 while (1) {
    status = _fetch_parcel("FETCH", dbcp, req);
    if (status == 1 && dbcp->fet_parcel_flavor == PclRECORD)
       return dbcp->fet_data_ptr;
    if (status == 1 && (dbcp->fet_parcel_flavor == PclDATAINFO ||
        dbcp->fet_parcel_flavor == PclENDSTATEMENT))
       continue;
    else
       return (char *) 0;
 }
}

/*--------------------------------------------------------------------
**  CLOSE
**------------------------------------------------------------------*/
int Zclose (
  pRequest   req )
{
 struct DBCAREA * dbcp;
 int  erq;

 dbcp = req->dbcp;
 dbcp->change_opts = 'N';
 dbcp->i_req_id = req->req_num;

  /* End the request. */
 dbcp->func = DBFERQ;
 DBCHCL(&result,cnta, dbcp);
 erq = check_cli_error("CLOSE", dbcp);

 return(erq);
}

/*--------------------------------------------------------------------
**  ABORT (asynchronous)
**------------------------------------------------------------------*/
int Zabort (
  pSession sess )
{
 struct DBCAREA * dbcp;

 dbcp = &(sess->dbc);

 dbcp->change_opts = 'N';

 dbcp->func = DBFABT;
 DBCHCL(&result,cnta, dbcp);

 return _fetch_all_parcels("ABORT", dbcp, 0);
}
