#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"
#include "tdsql.h"

 /* Global variables shared by subroutines */
int g_msglevel;
double g_activcount;
int g_errorcode;
char g_errormsg[260];
 /* Data descriptors for Results */
struct datadescr ddesc[3];

 /* Common variables within this file only */
static SV * c_msgl_sv;
static SV * c_actv_sv;
static SV * c_errc_sv;
static SV * c_emsg_sv;
 /* Many applications will use only one request at a time,
    so we pre-allocate this one to avoid having to allocate
    and free all the time.  */
Request  request0;
int  r0_in_use = 0;  /* Is it in use? */


static int
not_here(char *s)
{
    croak("%s not implemented on this architecture", s);
    return -1;
}


MODULE = Teradata::SQL		PACKAGE = Teradata::SQL


 # CONNECT to Teradata
int
Xconnect(log, ccs, tmode)
    PROTOTYPE:$$$
    INPUT:
	char *	 	log
	char *		ccs
	char *		tmode
    PREINIT:
	pSession        sess_ptr;
	int             ok;
    CODE:
	c_msgl_sv = get_sv("Teradata::SQL::msglevel", FALSE);
	c_actv_sv = get_sv("Teradata::SQL::activcount", FALSE);
	c_errc_sv = get_sv("Teradata::SQL::errorcode", FALSE);
	c_emsg_sv = get_sv("Teradata::SQL::errormsg", FALSE);
	g_msglevel = SvIV(c_msgl_sv);

	New(0, sess_ptr, 1, Session);
	ok = Zconnect(sess_ptr, log, ccs, tmode);
	if (ok) {
	   RETVAL = (int) sess_ptr;
	} else {
	   RETVAL = 0;
	   Safefree(sess_ptr);
	}

	sv_setnv(c_actv_sv, g_activcount);
	sv_setiv(c_errc_sv, g_errorcode);
	sv_setpv(c_emsg_sv, g_errormsg);
    OUTPUT:
	RETVAL

 # DISCONNECT
int
Xdisconnect(sess)
    PROTOTYPE:$
    INPUT:
	int	sess
    CODE:
	g_msglevel = SvIV(c_msgl_sv);
	RETVAL = Zdisconnect((pSession) sess);
	Safefree((pSession) sess);

	sv_setnv(c_actv_sv, g_activcount);
	sv_setiv(c_errc_sv, g_errorcode);
	sv_setpv(c_emsg_sv, g_errormsg);
    OUTPUT:
	RETVAL

 # EXECUTE without arguments
int
Xexecute(sess, sql)
    PROTOTYPE:$$
    INPUT:
	int		sess
	char *		sql
    CODE:
	g_msglevel = SvIV(c_msgl_sv);
	RETVAL = Zexecute((pSession) sess, sql);

	sv_setnv(c_actv_sv, g_activcount);
	sv_setiv(c_errc_sv, g_errorcode);
	sv_setpv(c_emsg_sv, g_errormsg);
    OUTPUT:
	RETVAL

 # OPEN without arguments
int
Xopen(sess, sql)
    PROTOTYPE:$$
    INPUT:
	int		sess
	char *		sql
    PREINIT:
	pRequest        req_ptr;
	 /* using_r0 = are we using request0 during this call? */
	int             using_r0, ok;
    CODE:
	g_msglevel = SvIV(c_msgl_sv);
	if (r0_in_use) {
	   using_r0 = 0;
	   New(0, req_ptr, 1, Request);
	} else {
	   using_r0 = 1;
	   req_ptr = &request0;
	   r0_in_use = 1;
	}
	req_ptr->dbcp = &(((pSession) sess)->dbc);

	ok = Zopen(req_ptr, sql);
	if (ok) {
	   RETVAL = (int)req_ptr;
	} else {
	   RETVAL = 0;
	   if (using_r0) {
	      memset(&request0, 0x00, sizeof(Request));
	      r0_in_use = 0;
	   } else {
	      Safefree(req_ptr);
	   }
	}

	sv_setnv(c_actv_sv, g_activcount);
	sv_setiv(c_errc_sv, g_errorcode);
	sv_setpv(c_emsg_sv, g_errormsg);
    OUTPUT:
	RETVAL

 # EXECUTE a prepared request with optional arguments
int
Xexecutep(sess, sql, ...)
    PROTOTYPE:$$
    INPUT:
	int		sess
	char *		sql
    PREINIT:
	int             i, wint, nindic, idlen, nargs;
	char *		sptr;
	STRLEN		slen;
	double		wdouble;
	Byte		hv_data[MAX_RDA_LEN];
	Byte *		hvdata_ptr;
	Byte *		hvindic_ptr;
	Byte		indic_mask;
	struct ModCliDataInfoType  hv_datainfo;
	struct ModCliDInfoType * hv_datainfo_ptr;
    CODE:
	g_msglevel = SvIV(c_msgl_sv);
	if (items == 2) {
	   RETVAL = Zexecutep((pSession) sess, sql);
	} else {
	    /* Store the Perl variables in an IndicData array. */
	    /* First, reserve the indicator bytes. */
	   nargs = items - 2;
	   nindic = (nargs + 7) / 8;
	   idlen = nindic;  /* IndicData length in bytes */
	   hvindic_ptr = hv_data;
	   *hvindic_ptr = 0x00;
	   indic_mask = 0x80;
	   hvdata_ptr = hv_data + nindic;

	    /* DataInfo */
	   hv_datainfo.FieldCount = (PclWord) nargs;
	   hv_datainfo_ptr = &(hv_datainfo.InfoVar[0]);

	   for (i = 2; i < items; i++) {
	      if ( SvIOK(ST(i)) ) {
	         hv_datainfo_ptr->SQLType = INTEGER_N;
	         hv_datainfo_ptr->SQLLen = 4;
	         wint = SvIV(ST(i));
	         memcpy(hvdata_ptr, &wint, 4);
	         hvdata_ptr += 4;
	         idlen += 4;
	      } else if ( SvNOK(ST(i)) ) {
	         hv_datainfo_ptr->SQLType = FLOAT_N;
	         hv_datainfo_ptr->SQLLen = 8;
	         wdouble = SvNV(ST(i));
	         memcpy(hvdata_ptr, &wdouble, 8);
	         hvdata_ptr += 8;
	         idlen += 8;
	      } else if ( SvPOK(ST(i)) ) {
	         sptr = SvPV(ST(i), slen);
	         hv_datainfo_ptr->SQLType = CHAR_N;
	         hv_datainfo_ptr->SQLLen = slen;
	         memcpy(hvdata_ptr, sptr, slen);
	         hvdata_ptr += slen;
	         idlen += slen;
	      } else {  /* Null */
	         hv_datainfo_ptr->SQLType = INTEGER_N;
	         hv_datainfo_ptr->SQLLen = 4;
	         wint = 0;
	         memcpy(hvdata_ptr, &wint, 4);
	         *hvindic_ptr |= indic_mask;
	         hvdata_ptr += 4;
	         idlen += 4;
	      }
	       /* Point to the next DataInfo field. */
	      hv_datainfo_ptr++;
	       /* Point to the next indicator bit. */
	      if (indic_mask != 0x01) {
	         indic_mask >>= 1;
	      } else {
	         indic_mask = 0x80;
	         hvindic_ptr++;
	      }
	   }
	   RETVAL = Zexecutep_args((pSession) sess, sql, &hv_datainfo,
	     hv_data, idlen);
	}
	sv_setnv(c_actv_sv, g_activcount);
	sv_setiv(c_errc_sv, g_errorcode);
	sv_setpv(c_emsg_sv, g_errormsg);
    OUTPUT:
	RETVAL

 # OPEN without arguments, segmented
int
Xopenseg(sess, sql, save_spl)
    PROTOTYPE:$$
    INPUT:
	int		sess
	char *		sql
	char *		save_spl
    PREINIT:
	pRequest        req_ptr;
	 /* using_r0 = are we using request0 during this call? */
	int             using_r0, ok;
    CODE:
	g_msglevel = SvIV(c_msgl_sv);
	if (r0_in_use) {
	   using_r0 = 0;
	   New(0, req_ptr, 1, Request);
	} else {
	   using_r0 = 1;
	   req_ptr = &request0;
	   r0_in_use = 1;
	}
	req_ptr->dbcp = &(((pSession) sess)->dbc);

	ok = Zopenseg(req_ptr, sql, save_spl);
	if (ok) {
	   RETVAL = (int)req_ptr;
	} else {
	   RETVAL = 0;
	   if (using_r0) {
	      memset(&request0, 0x00, sizeof(Request));
	      r0_in_use = 0;
	   } else {
	      Safefree(req_ptr);
	   }
	}

	sv_setnv(c_actv_sv, g_activcount);
	sv_setiv(c_errc_sv, g_errorcode);
	sv_setpv(c_emsg_sv, g_errormsg);
    OUTPUT:
	RETVAL

 # OPEN a prepared request with optional arguments
int
Xopenp(sess, sql, ...)
    PROTOTYPE:$$
    INPUT:
	int		sess
	char *		sql
    PREINIT:
	int             i, wint, nindic, idlen, nargs;
	 /* using_r0 = are we using request0 during this call? */
	int             using_r0, ok;
	pRequest	req_ptr;
	char *		sptr;
	STRLEN		slen;
	double		wdouble;
	Byte		hv_data[MAX_RDA_LEN]; /* "host variables" */
	Byte *		hvdata_ptr;
	Byte *		hvindic_ptr;
	Byte		indic_mask;
 	struct ModCliDataInfoType  hv_datainfo;
	struct ModCliDInfoType * hv_datainfo_ptr;
    CODE:
	g_msglevel = SvIV(c_msgl_sv);
	if (r0_in_use) {
	   using_r0 = 0;
	   New(0, req_ptr, 1, Request);
	} else {
	   using_r0 = 1;
	   req_ptr = &request0;
	   r0_in_use = 1;
 	}
	req_ptr->dbcp = &(((pSession) sess)->dbc);
	if (items == 2) {
	   ok = Zopenp(req_ptr, sql);

	} else {
	    /* Store the Perl variables in an IndicData array. */
	    /* First, reserve the indicator bytes. */
	   nargs = items - 2;
	   nindic = (nargs + 7) / 8;
	   idlen = nindic;  /* IndicData length in bytes */
	   hvindic_ptr = hv_data;
	   *hvindic_ptr = 0x00;
	   indic_mask = 0x80;
	   hvdata_ptr = hv_data + nindic;

	    /* DataInfo */
	   hv_datainfo.FieldCount = (PclWord) nargs;
	   hv_datainfo_ptr = &(hv_datainfo.InfoVar[0]);

	   for (i = 2; i < items; i++) {
	      if ( SvIOK(ST(i)) ) {
	         hv_datainfo_ptr->SQLType = INTEGER_N;
	         hv_datainfo_ptr->SQLLen = 4;
	         wint = SvIV(ST(i));
	         memcpy(hvdata_ptr, &wint, 4);
	         hvdata_ptr += 4;
	         idlen += 4;
	      } else if ( SvNOK(ST(i)) ) {
	         hv_datainfo_ptr->SQLType = FLOAT_N;
	         hv_datainfo_ptr->SQLLen = 8;
	         wdouble = SvNV(ST(i));
	         memcpy(hvdata_ptr, &wdouble, 8);
	         hvdata_ptr += 8;
	         idlen += 8;
	      } else if ( SvPOK(ST(i)) ) {
	         sptr = SvPV(ST(i), slen);
	         hv_datainfo_ptr->SQLType = CHAR_N;
	         hv_datainfo_ptr->SQLLen = slen;
	         memcpy(hvdata_ptr, sptr, slen);
	         hvdata_ptr += slen;
	         idlen += slen;
	      } else {  /* Null */
	         hv_datainfo_ptr->SQLType = INTEGER_N;
	         hv_datainfo_ptr->SQLLen = 4;
	         wint = 0;
	         memcpy(hvdata_ptr, &wint, 4);
	         *hvindic_ptr |= indic_mask;
	         hvdata_ptr += 4;
	         idlen += 4;
	      }
	       /* Point to the next DataInfo field. */
	      hv_datainfo_ptr++;
	       /* Point to the next indicator bit. */
	      if (indic_mask != 0x01) {
	         indic_mask >>= 1;
	      } else {
	         indic_mask = 0x80;
	         hvindic_ptr++;
	      }
	   }
	   ok = Zopenp_args(req_ptr, sql,
	     &hv_datainfo, hv_data, idlen);
	}
	if (ok) {
	   RETVAL = (int)req_ptr;
	} else {
	   RETVAL = 0;
	   if (using_r0) {
	      memset(&request0, 0x00, sizeof(Request));
	      r0_in_use = 0;
	   } else {
	      Safefree(req_ptr);
	   }
	}

	sv_setnv(c_actv_sv, g_activcount);
	sv_setiv(c_errc_sv, g_errorcode);
	sv_setpv(c_emsg_sv, g_errormsg);
    OUTPUT:
	RETVAL

 # FETCH. Last argument says whether this is fetching into a
 # hash or not.
void
Xfetch(req, hash)
    PROTOTYPE:$$
    INPUT:
	int		req
	int		hash
    PREINIT:
	int		i, decp, decs;
	STRLEN		slen;
	char *		sptr;
	char *		ret_data;
	Byte *		indic_ptr;
	Byte *		data_ptr;
	struct datadescr * ddesc_ptr;
	long int	wint;
        double		wdouble;
	char		wstring[24];
	Byte		indic_mask;
    PPCODE:
	g_msglevel = SvIV(c_msgl_sv);
	ret_data = Zfetch((pRequest) req);

	if (ret_data) {
	   ddesc_ptr = &(((pRequest) req)->ddesc);
	    /* Point to the indicators and the data. */
	   indic_ptr = (Byte *) ret_data;
	   indic_mask = 0x80;
	   data_ptr = ((Byte *) ret_data) + ( (ddesc_ptr->nfields + 7) / 8);

	   for (i = 0; i < ddesc_ptr->nfields; i++) {
	       /* If this is a hash request, push the name first. */
	      if (hash) {
	         slen = strlen(ddesc_ptr->sqlvar[i].colident);
	         sptr = (char *)ddesc_ptr->sqlvar[i].colident;
	         XPUSHs(sv_2mortal(newSVpv(sptr, slen)));
	      }

	       /* Now push the value, testing for null first. */
	      switch (ddesc_ptr->sqlvar[i].sqltype) {
	       case INTEGER_N:
	          if ( (*indic_ptr & indic_mask) > 0) { /* Null */
	             XPUSHs(&PL_sv_undef);
	          } else {
	             wint = *((long *)data_ptr) + 0;
	             XPUSHs(sv_2mortal(newSViv(wint)));
	          }
	          data_ptr += 4;
	          break;
	       case SMALLINT_N:
	          if ( (*indic_ptr & indic_mask) > 0) {
	             XPUSHs(&PL_sv_undef);
	          } else {
	             wint = *((short *)data_ptr) + 0;
	             XPUSHs(sv_2mortal(newSViv(wint)));
	          }
	          data_ptr += 2;
	          break;
	       case BYTEINT_N:
	          if ( (*indic_ptr & indic_mask) > 0) {
	             XPUSHs(&PL_sv_undef);
	          } else {
	             wint = *((ByteInt *)data_ptr) + 0;
	             XPUSHs(sv_2mortal(newSViv(wint)));
	          }
	          data_ptr++;
	          break;
	       case CHAR_N:
	          slen = ddesc_ptr->sqlvar[i].datalen;
	          if ( (*indic_ptr & indic_mask) > 0) {
	             XPUSHs(&PL_sv_undef);
	          } else {
	             sptr = (char *) data_ptr;
	             XPUSHs(sv_2mortal(newSVpv(sptr, slen)));
	          }
	          data_ptr += slen;
	          break;
	       case VARCHAR_N:
	          slen = *((unsigned short *) data_ptr);
	          if ( (*indic_ptr & indic_mask) > 0) {
	             XPUSHs(&PL_sv_undef);
	          } else {
	             sptr = (char *) (data_ptr + 2);
	             XPUSHs(sv_2mortal(newSVpv(sptr, slen)));
	          }
	          data_ptr += slen + 2;
	          break;
	       case DECIMAL_N:
	           /* Decimal precision and scale */
	          decp = ddesc_ptr->sqlvar[i].datalen;
	          decs = ddesc_ptr->sqlvar[i].decscale;

	          if ( (*indic_ptr & indic_mask) > 0) {
	             XPUSHs(&PL_sv_undef);
	             data_ptr += ddesc_ptr->sqlvar[i].dlb;
	          } else if (decp <= 9) {
	             wdouble = _dec_to_double(data_ptr, decp, decs);
	             XPUSHs(sv_2mortal(newSVnv(wdouble)));
	             data_ptr += ddesc_ptr->sqlvar[i].dlb;
	          } else if (decp <= 18) {
	             _dec_to_string(wstring, data_ptr, decs);
	             slen = strlen(wstring);
	             XPUSHs(sv_2mortal(newSVpv(wstring, slen)));
	             data_ptr += 8;
	          } else {
		     if (g_msglevel > 0)
	                warn("Decimal field too large");
	             XPUSHs(sv_2mortal(newSVnv(0.0)));
	             data_ptr += 16;
	          }
	          break;
	       case BIGINT_N:
	          if ( (*indic_ptr & indic_mask) > 0) {
	             XPUSHs(&PL_sv_undef);
	          } else {
	             _dec_to_string(wstring, data_ptr, 0);
	             slen = strlen(wstring);
	             XPUSHs(sv_2mortal(newSVpv(wstring, slen)));
	          }
	          data_ptr += 8;
	          break;
	       case FLOAT_N:
	          if ( (*indic_ptr & indic_mask) > 0) {
	             XPUSHs(&PL_sv_undef);
	          } else {
	             wdouble = *((double *) data_ptr);
	             XPUSHs(sv_2mortal(newSVnv(wdouble)));
	          }
	          data_ptr += 8;
	          break;
	       default:
	          croak("Data type %d not supported\n",
	           ddesc_ptr->sqlvar[i].sqltype);
	      }

	        /* Point to the next indicator bit. */
	      if (indic_mask != 0x01) {
	         indic_mask >>= 1;
	      } else {
	         indic_mask = 0x80;
	         indic_ptr++;
	      }
	   }
	}

	sv_setnv(c_actv_sv, g_activcount);
	sv_setiv(c_errc_sv, g_errorcode);
	sv_setpv(c_emsg_sv, g_errormsg);

 # CLOSE
int
Xclose(req)
    PROTOTYPE:$
    INPUT:
	int		req
    CODE:
	g_msglevel = SvIV(c_msgl_sv);
	RETVAL = Zclose((pRequest) req);
	if ((pRequest)req == &request0) {
	   memset(&request0, 0x00, sizeof(Request));
	   r0_in_use = 0;
	} else {
	   Safefree((pRequest) req);
	}

	sv_setnv(c_actv_sv, g_activcount);
	sv_setiv(c_errc_sv, g_errorcode);
	sv_setpv(c_emsg_sv, g_errormsg);
    OUTPUT:
	RETVAL

 # ABORT (DBFABT)
int
Xabort(sess)
    PROTOTYPE:$
    INPUT:
	int		sess
    CODE:
	g_msglevel = SvIV(c_msgl_sv);
	RETVAL = Zabort((pSession) sess);

	sv_setnv(c_actv_sv, g_activcount);
	sv_setiv(c_errc_sv, g_errorcode);
	sv_setpv(c_emsg_sv, g_errormsg);
    OUTPUT:

 # SERVER_INFO (DBCHQE)
void
Xserver_info(server, qepitem)
    PROTOTYPE:$$
    INPUT:
	char *		server
	int		qepitem
    PREINIT:
	int             wint, string_len;
	int             ok;
	 /* DBCHQE request area */
	DBCHQEP		our_qep;
	 /* Data returned from DBCHQE */
	Byte		hqe_ret_data[200];
	char *		RArea_char_ptr;
	char *		ret_ptr;
	 /* Error message from DBCHQE */
	char 		hqe_message[258];
	struct QEPDBLIMIT_  our_dblimit;
    PPCODE:
	c_msgl_sv = get_sv("Teradata::SQL::msglevel", FALSE);
	c_errc_sv = get_sv("Teradata::SQL::errorcode", FALSE);
	c_emsg_sv = get_sv("Teradata::SQL::errormsg", FALSE);
	g_msglevel = SvIV(c_msgl_sv);

	  /* Store the fields needed for the request. */
	our_qep.qepLevel = QEPL10NLVL1;
	our_qep.qepItem = qepitem;
	our_qep.qepTLen = strlen(server);
	our_qep.qepTDP = (Int32) server;
	our_qep.qepRALen = 200;
	our_qep.qepRArea = hqe_ret_data;
	memcpy(our_qep.qepMLid, "EN", 2);
	our_qep.qepMsgP = hqe_message;
	our_qep.qepMsgM = 256;

	ok = Zserver_info(&our_qep);
	if (ok) {
	   RArea_char_ptr = (char *) our_qep.qepRArea;
	   switch (our_qep.qepItem) {
	    case QEPIDBR:	/* DBS release info */
	      /* Returned to perl as a two-element list */
	      ret_ptr = RArea_char_ptr;
	      for (string_len=0; string_len < 30; string_len++) {
		 if (*ret_ptr == ' ') break;
		 else ret_ptr++;
	      }
	      XPUSHs(sv_2mortal(newSVpv(RArea_char_ptr, string_len)));
	      RArea_char_ptr += 30;
	      ret_ptr = RArea_char_ptr;
	      for (string_len=0; string_len < 32; string_len++) {
		 if (*ret_ptr == ' ') break;
		 else ret_ptr++;
	      }
	      XPUSHs(sv_2mortal(newSVpv(RArea_char_ptr, string_len)));
	      break;
	    case QEPISC:	/* char set */
	    case QEPICL2R:	/* CLIv2 Release Info */
	    case QEPIASL:	/* server default char set */
	    case QEPIDCS:	/* server default char set */
	      XPUSHs(sv_2mortal(newSVpv(RArea_char_ptr,
		 our_qep.qepRDLen)));
	      break;
	    case QEPIFTSM:	/* transaction semantics */
	    case QEPIFLCS:	/* lang-conformance support */
	    case QEPIFUCR:	/* updatable cursor support */
	    case QEPIFRFI:	/* referential integ. support */
	    case QEPIDTSM:	/* tx_semantics default */
	    case QEP64K:	/* 64KB parcel support */
	    case QEPIFSSO:	/* SSO support */
	    case QEPIFUPS:	/* Atomic UPSERT support */
	    case QEPIAOP:	/* array-ops support */
	    case QEPIFMRG:	/* Merge-Into support */
	    case QEPIFLOB:	/* LOB support */
	    case QEPIXRS:	/* extended response support */
	    case QEPIUD:	/* Identity Column Support */
	    case QEPIRPO:	/* cursor positioning sup. */
	    case QEPIESS:	/* Enhanced Statement Status */
	    case QEPIUDT:	/* User-defined types */
	    case QEPIRCA:	/* relaxed call arguments */
	      XPUSHs(sv_2mortal(newSVpv(our_qep.qepRArea, 1)));
	      break;
	    case QEPIDPF:	/* internal (V)AMP count */
	    case QEPIDMSS:	/* MaxSegmentSize */
	      wint = *((int *)our_qep.qepRArea);
	      XPUSHs(sv_2mortal(newSViv(wint)));
	      break;
	    case QEPIEPU:	/* enlarged parcel (APH) support */
	    case QEPIAPH:	/* APH responses */
	      wint = *((short int *)our_qep.qepRArea);
	      XPUSHs(sv_2mortal(newSViv(wint)));
	      break;
	    case QEPITPR:	/* precision on timestamp */
	      wint = *((short *)RArea_char_ptr);
	      XPUSHs(sv_2mortal(newSViv(wint)));
	      wint = *((short *)(RArea_char_ptr + 2));
	      XPUSHs(sv_2mortal(newSViv(wint)));
	      break;
	    case QEPISQL:	/* SQL limits */
	      memcpy(&our_dblimit, our_qep.qepRArea,
	        sizeof(struct QEPDBLIMIT_));
	      wint = (int) our_dblimit.MaxRowBytes;
	      XPUSHs(sv_2mortal(newSViv(wint)));
	      wint = (int) our_dblimit.MaxLobBytes;
	      XPUSHs(sv_2mortal(newSViv(wint)));
	      XPUSHs(sv_2mortal(newSViv(our_dblimit.MaxObjectNameChars)));
	      XPUSHs(sv_2mortal(newSViv(our_dblimit.MaxColinTbl)));
	      XPUSHs(sv_2mortal(newSViv(our_dblimit.MaxTblinSel)));
	      XPUSHs(sv_2mortal(newSViv(our_dblimit.MaxColinSel)));
	      XPUSHs(sv_2mortal(newSViv(our_dblimit.MaxColGrpBy)));
	      XPUSHs(sv_2mortal(newSViv(our_dblimit.MaxColOrdrBy)));
	      XPUSHs(sv_2mortal(newSViv(our_dblimit.MaxCharLiteralChars)));
	      XPUSHs(sv_2mortal(newSViv(our_dblimit.MaxBinLiteralChars)));
	      XPUSHs(sv_2mortal(newSViv(our_dblimit.MaxColBytes)));
	      XPUSHs(sv_2mortal(newSViv(our_dblimit.MaxCharChars)));
	      XPUSHs(sv_2mortal(newSViv(our_dblimit.MaxVarcharChars)));
	      XPUSHs(sv_2mortal(newSViv(our_dblimit.MaxGraphicChars)));
	      XPUSHs(sv_2mortal(newSViv(our_dblimit.MaxVargraphicChars)));
	      XPUSHs(sv_2mortal(newSViv(our_dblimit.MaxByteBytes)));
	      XPUSHs(sv_2mortal(newSViv(our_dblimit.MaxVarbyteBytes)));
	      XPUSHs(sv_2mortal(newSViv(our_dblimit.MaxDecimal)));
	      XPUSHs(sv_2mortal(newSViv(our_dblimit.MaxTimeScale)));
	      XPUSHs(sv_2mortal(newSViv(our_dblimit.MaxTimeStampScale)));
	      XPUSHs(sv_2mortal(newSViv(our_dblimit.MaxIntervalToSecondScale)));
	      XPUSHs(sv_2mortal(newSViv(our_dblimit.MaxFldsUsingRow)));
	      XPUSHs(sv_2mortal(newSViv(our_dblimit.MaxParamsInRequest)));
	      XPUSHs(sv_2mortal(newSViv(our_dblimit.MaxSPParams)));
	      break;
	    /*case QEPIACS:   not implemented */
	    /* remainder are undocumented or not implemented here */
	    default:
	      XPUSHs(&PL_sv_undef);
	   }
	} else {
	   XPUSHs(&PL_sv_undef);
	}

	sv_setiv(c_errc_sv, g_errorcode);
	sv_setpv(c_emsg_sv, g_errormsg);
