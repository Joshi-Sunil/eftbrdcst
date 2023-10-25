using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using System.Text;
using System.Data.SqlClient;
using System.Data.OracleClient;
using System.Net.Mail;

/*
 * This application runs to broadcast messages related to the completion of the daily EFT run.
 * It accesses tables within the local PMTREPORTING database for control information.  The EFT
 * Data is obtained from the P290 Oracle database.
 * */

namespace EFTBrdcst
{
    class EFTBrdcst
    {
		static private int ExitValue;
        static private bool fHelp=false;
		static public bool DebugFlag = false;
        static public bool fNoticeOnly = false;
        static private string DefaultServer = "NALD3000";
		static public string DatabaseServer;
        static private string threshhold = "1000";
        static string TemplateServer = "MCKPH-016";
        static string InternalTemplateFilename;
        static private int TotalBasicSubscribers = 0;
        static private int TotalAdvancedSubscribers = 0;
        static private int TotalEmailsSent = 0;
        static private int TotalBasicEmailsSent = 0;
        static private int TotalAdvEmailsSent = 0;
        static private int TotalInternalNoticeRcpts = 0;

        static private string pmtdate;

        static private ArrayList Subs = new ArrayList();

        static string MbrList;
        static string EFTList;
        static string EFTSummary;
        static string PayerList;

        static string DetailLog = "";
        static string NL = "\n";
        static bool fError = false;

        static string SMTPServer = "DDCMAIL.mckesson.com";

        // MS SQL server connections
        static private SqlConnection dbConnection = null;
        //  Oracle connections
        static OracleConnection OracleDbConnection = null;
        static string OracleInstance = "P290";

        static void Main(string[] args)
        {
            bool retval;

            ExitValue = 0;

            retval = get_args(args);
            if (retval)
            {
                if (DebugFlag) DetailLog += "Database: [" + DatabaseServer + "]";

                if (openDatabases())
                {
                    if (!fNoticeOnly)
                    {
                        BasicEFTBroadcast();

                        AdvEFTBroadcast();
                    }

                    InternalEFTNotice();

                    LaunchAHMailer();

                    closeDatabases();
                }
                else
                {
                    ExitValue = 1;
                }
            }

            if (!fHelp) OutputRunSummary();

            Environment.ExitCode = ExitValue;

        }
		static bool get_args(string[] args)
		{
			bool retval = true;

            pmtdate = System.DateTime.Today.ToShortDateString();

			DebugFlag = false;
            DatabaseServer = DefaultServer;
			if (args.Length < 0 || args.Length > 9)
			{
				retval = false;
			}
			else 
			{
				for(int i = 0; i < args.Length; i++)
				{
					string sw = args[i].Substring(0,2);
					string val = args[i].Substring(2);
					if (sw.CompareTo("-s") == 0)
					{
						DatabaseServer = val;
					}
                    else if (sw.CompareTo("-t") == 0)
                    {
                        threshhold = val;
                    }
                    else if (sw.CompareTo("-f") == 0)
                    {
                        TemplateServer = val;
                    }
                    else if (sw.CompareTo("-p") == 0)
                    {
                        pmtdate = val;
                    }
                    else if (sw.CompareTo("-d") == 0)
                    {
                        DebugFlag = true;
                    }
                    else if (sw.CompareTo("-n") == 0)
                    {
                        fNoticeOnly = true;
                    }
                    else if (sw.CompareTo("-?") == 0)
                    {
                        ExitValue = 1;
                        fHelp = true;
                        retval = false;
                    }
                    else
                    {
                        Console.WriteLine("Arg[{0}] = [{1}]", i, args[i]);
                        Console.WriteLine("? Unrecognized switch: " + sw + ", value: " + val);
                        ExitValue = 1;
                        retval = false;
                    }

				}

			}
			if (retval == false || fHelp)
			{
				Console.WriteLine("Usage:");
				Console.WriteLine("  EFTBrdcst [-s<servername>] [-t<value>] [-d] [-p<mm/dd/yyyy>] [-f<folder>] [-n]");
				Console.WriteLine("    -s               Database server name switch");
				Console.WriteLine("    <servername>     Name of database server (default is NALD3000)");
                Console.WriteLine("    -t               Threshhold for PBM reporting in internal notice");
                Console.WriteLine("    <value>          Value of threshhold (default=1000)");
				Console.WriteLine("    -d               Debug flag");
                Console.WriteLine("    -f               Template server switch");
                Console.WriteLine("    <tserver>        server for templates (default is MCKPH-016)");
                Console.WriteLine("    -p               Payment date switch (for testing)");
                Console.WriteLine("    <mm/dd/yyyy>     Date for test run of broadcast");
                Console.WriteLine("    -n               Internal Notice Only Flag");
				Console.WriteLine("");
			}

			return retval;

		}
        static bool openDatabases()
        {
            bool retval;

            retval = openMSSqlDatabase();
            if (retval)
            {
                if (openOracleConnection())
                    retval = true;
                else
                {
                    closeMSSqlDatabase();
                    retval = false;
                }
            }
            else
                retval = false;

            return retval;
        }
        static void closeDatabases()
        {
            closeMSSqlDatabase();
            closeOracleDatabase();
        }
        static bool openMSSqlDatabase()
        {
            bool retval = false;
            string sSqlConnectionString = "Persist Security Info=True;" +
                ";Initial Catalog=PMTReporting" +
                ";Data Source=" + DatabaseServer +
                ";Trusted_Connection = Yes;";
                // +
                //";User ID=PBMloader" +
                //";Password=load835data";

            try
            {
                dbConnection = new SqlConnection(sSqlConnectionString);
                dbConnection.Open();
                retval = true;
            }
            catch (Exception ex)
            {
                DetailLog += "? Error opening database" + NL;
                DetailLog += sSqlConnectionString + NL;
                DetailLog += "  "+ex.Message + NL;
                fError = true;
                ExitValue = 6;
            }

            DetailLog += "Opened MS SQL" + NL;
            return retval;
        }
        static bool closeMSSqlDatabase()
        {
            if (dbConnection != null)
            {
                try
                {
                    dbConnection.Close();
                }
                catch (Exception ex)
                {
                    DetailLog += "? Error closing database" + NL;
                    DetailLog += "  " + ex.Message.ToString() + NL;
                    fError = true;
                    return false;
                }
            }
            DetailLog += "Closed MS SQL" + NL;
            return true;

        }
        private static bool openOracleConnection()
        {
            try
            {

                Dictionary<string, string> dbConfig = getDBConfig();
                string oracleUser = "";
                string oraclePassword = "";

                if (dbConfig.ContainsKey("ORACLEUSER"))
                {
                    oracleUser = dbConfig["ORACLEUSER"];
                }
                if (dbConfig.ContainsKey("ORACLEPASSWORD"))
                {
                    oraclePassword = dbConfig["ORACLEPASSWORD"];
                }
                if (dbConfig.ContainsKey("ORACLEINSTANCE"))
                {
                    OracleInstance = dbConfig["ORACLEINSTANCE"];
                }


                OracleDbConnection = new OracleConnection("Data Source="
                    + OracleInstance + ";User Id="+oracleUser+";Password="+ oraclePassword + ";");
                OracleDbConnection.Open();
            }
            catch (Exception ex)
            {
                DetailLog += "? Error opening Oracle" + NL;
                DetailLog += "  " + ex.Message.ToString() + NL;
                fError = true;
                return false;
            }
            DetailLog += "Opened Oracle" + NL;
            return true;
        }


        private static void closeOracleDatabase()
        {
            if (OracleDbConnection != null)
            {
                try
                {
                    OracleDbConnection.Close();
                    OracleDbConnection.Dispose();
                }
                catch (Exception ex)
                {
                    DetailLog += "? Error closing Oracle";
                    DetailLog += "  " + ex.Message.ToString();
                    fError = true;
                }
            }
            DetailLog += "Closed Oracle" + NL;
        }
        private static void BasicEFTBroadcast()
        {
            SqlCommand sCmd = null;
            SqlDataReader sReader = null;
            string BasicTemplateFilename;
            bool fHTML;

            try
            {
                sCmd = dbConnection.CreateCommand();
                sCmd.CommandText = "select TemplateFilename,IsHTML from PMTREPORTING..Templates "
                    + "where Class = 'BAS'";
                sReader = sCmd.ExecuteReader();
                if (sReader.HasRows)
                {
                    sReader.Read();
                    BasicTemplateFilename = sReader["TemplateFilename"].ToString();
                    fHTML = false;
                    if (sReader["IsHTML"].ToString().CompareTo("Y") == 0)
                        fHTML = true;
                }
                else
                {
                    BasicTemplateFilename = "\\\\" + TemplateServer + "\\templates\\basic.txt";
                    fHTML = false;
                }
                DetailLog += "Basic Template: " + BasicTemplateFilename + "(" + fHTML.ToString() + ")" + NL;
                sReader.Close();
                sReader.Dispose();
                sCmd.Dispose();

            }
            catch (Exception ex)
            {
                DetailLog += "? Error getting basic Template" + NL;
                DetailLog += "  " + ex.Message.ToString() + NL;
                fError = true;
                if (sReader != null) sReader.Dispose();
                if (sCmd != null) sCmd.Dispose();
                return;
            }

            try
            {
                sCmd = dbConnection.CreateCommand();
                sCmd.CommandText = "select b.ncpdp_prvdr_num,b.EFT_Email_Rcpt " +
                    "from pmtreporting..EFTBrdcstBasic b ";
                sReader = sCmd.ExecuteReader();
                ArrayList maillist = new ArrayList();
                while (sReader.Read())
                {
                    string ncpdp = sReader["ncpdp_prvdr_num"].ToString();
                    TotalBasicSubscribers++;

                    maillist.Clear();
                    maillist.Add(sReader["EFT_Email_Rcpt"].ToString());
                    ArrayList EFTs = new ArrayList();

                    DetailLog += "Check payments for: "+ncpdp+"("+maillist[0]+")"+NL;
                    OracleCommand oCmd;
                    OracleDataReader oReader;

                    try
                    {
                        oCmd = OracleDbConnection.CreateCommand();
                        oCmd.CommandText = "select r.pmt_id,to_char(r.pmt_amt,'$999,990.00') pmt_amt, " +
                            "m.mbr_stor_nam " +
                            "from s_clm_pmt_rgstr r, s_ah_mbr m where " +
                            "r.mbr_id=m.mbr_id and m.ncpdp_prvdr_num = '" + ncpdp + "' " +
                            "and trunc(r.pmt_dts) = to_date('" + pmtdate + "','mm/dd/yyyy') " +
                            "and r.pmt_stat_cd != 'V' " +
                            "order by r.pmt_id";
                        oReader = oCmd.ExecuteReader();
                        if (!oReader.HasRows)
                        {
                            oReader.Close();
                            oReader.Dispose();
                            oCmd.Dispose();
                        }
                        else
                        {

                            EFTSummary = "   " + "PMT".PadRight(12) + "NCPDP".PadRight(8) + "Amount".PadLeft(12) + " Store Name\n\n";
                            while (oReader.Read())
                            {

                                EFTSummary +=
                                    "   " +
                                    oReader["PMT_ID"].ToString().PadRight(12) +
                                    ncpdp.PadRight(8) +
                                    oReader["PMT_AMT"].ToString().PadLeft(12) +
                                    " " + oReader["MBR_STOR_NAM"] +
                                    "\n";
                                EFTs.Add(oReader[0]);

                            }
                            oReader.Close();
                            oReader.Dispose();

                            if (EFTs.Count > 0)
                            {
                                EFTList = "";
                                for (int i = 0; i < EFTs.Count; i++)
                                    if (i == 0) EFTList = "'" + EFTs[i].ToString() + "'";
                                    else EFTList += ", '" + EFTs[i].ToString() + "'";

                                oCmd.CommandText = "select plan_nam from s_ah_plan " +
                                    " where ah_plan_id != 'AHADJ001' and ah_plan_id in (" +
                                    " select distinct (ah_plan_id) from s_store_clm where pmt_id in (" + EFTList + ") " +
                                    ")";
                                oReader = oCmd.ExecuteReader();

                                PayerList = "  Payers: \n";
                                while (oReader.Read())
                                {
                                    PayerList += "     " + oReader["PLAN_NAM"] + "\n";
                                }

                                oReader.Close();
                                oReader.Dispose();

                                SendNotice(ncpdp, maillist, BasicTemplateFilename, fHTML);
                                DetailLog += "Mail Basic Notice: "+ncpdp+"("+maillist[0]+")"+NL;
                                TotalBasicEmailsSent++;
                            }


                        }
                        oCmd.Dispose();
                    }
                    catch (Exception ex)
                    {
                        DetailLog += "? Error checking basic payment"+NL;
                        DetailLog += "  " + ex.Message.ToString() + NL;
                        fError = true;
                    }


                }
                sReader.Close();
                sReader.Dispose();
                sCmd.Dispose();
            }
            catch (Exception ex)
            {
                DetailLog += "? Error getting Basic subscriber list" + NL;
                DetailLog += "  " + ex.Message.ToString() + NL;
                fError = true;
            }
        }

        private static void AdvEFTBroadcast()
        {
            SqlCommand SubList = dbConnection.CreateCommand();
            SqlDataReader SubReader=null;

            try
            {
                SubList.CommandText = "select SubscrID from EFTBrdcstSubs order by SubscrID";
                SubReader = SubList.ExecuteReader();
                Subs.Clear();
                while (SubReader.Read())
                {
                    Subs.Add(SubReader.GetValue(SubReader.GetOrdinal("SubscrID")).ToString());
                }
            }
            catch (Exception ex)
            {
                DetailLog += "? Error getting Advanced EFT Notice list" + NL;
                DetailLog += "  "+ex.Message.ToString() + NL;
                fError = true;
                if (!(SubReader == null))
                {
                    SubReader.Close();
                    SubReader.Dispose();
                }
                return;
            }
            SubReader.Close();
            SubReader.Dispose();
            SubList.Dispose();

            for (int i = 0; i < Subs.Count; i++)
            {
                try
                {
                    //Console.WriteLine("  " + Subs[i]);
                    ReportPaymentsforSub(Subs[i].ToString());
                }
                catch (Exception ex)
                {
                    DetailLog += "? Error in ReportPaymentsforSub("+Subs[i].ToString()+")"+NL;
                    DetailLog += "  " + ex.Message.ToString() + NL;
                    fError = true;
                }
            }

        }
        private static void ReportPaymentsforSub(string SubscriberID)
        {
            ArrayList Members = new ArrayList();
            ArrayList EFTs = new ArrayList();
            string showpayerdetails = "N";
            string showEFTTotal = "N";
            string showEFTDetails = "N";
            bool payerDetails = false;
            bool EFTTotal = false;
            bool EFTDetails = false;

            SqlCommand sCmd;
            SqlDataReader sReader;
            OracleCommand oCmd = OracleDbConnection.CreateCommand();
            OracleDataReader oReader;

            MbrList = "";
            EFTList = "";
            EFTSummary = "";
            PayerList = "";

            //Get list of members and flags
            try
            {
                DetailLog += "Check payments for Subscriber: " + SubscriberID + NL;
                TotalAdvancedSubscribers++;

                sCmd = dbConnection.CreateCommand();
                sCmd.CommandText = "select id from pmtreporting..EFTBrdcstMembers where " +
                    "IDType = 'M' and SubscrID = '" + SubscriberID + "'";
                sReader = sCmd.ExecuteReader();
                while (sReader.Read()) Members.Add(sReader["id"]);
                sReader.Close();
                sReader.Dispose();
                sCmd.CommandText = "select id from pmtreporting..EFTBrdcstMembers where " +
                    "IDType = 'C' and SubscrID = '" + SubscriberID + "'";
                sReader = sCmd.ExecuteReader();
                while (sReader.Read())
                {
                    oCmd = OracleDbConnection.CreateCommand();
                    oCmd.CommandText = "select mbr_id from s_ah_mbr where ah_chn_id = '" + sReader["id"] + "'";
                    oReader = oCmd.ExecuteReader();
                    while (oReader.Read()) Members.Add(oReader["mbr_id"]);
                    oReader.Close();
                    oReader.Dispose();
                    oCmd.Dispose();
                }
                sReader.Close();
                sReader.Dispose();
                for (int i = 0; i < Members.Count; i++)
                    if (i == 0) MbrList += Members[i].ToString();
                    else MbrList += ", " + Members[i].ToString();

                // Get flags for details
                sCmd.CommandText = "select showpayerdetails,showEFTDetails,showEFTTotal from pmtreporting..EFTBrdcstSubs where " +
                    "SubscrID = '" + SubscriberID + "'";
                sReader = sCmd.ExecuteReader();
                while (sReader.Read())
                {
                    showpayerdetails = sReader["showpayerdetails"].ToString();
                    showEFTDetails = sReader["showEFTDetails"].ToString();
                    showEFTTotal = sReader["showEFTTotal"].ToString();
                }
                sReader.Close();
                sReader.Dispose();
                if (showpayerdetails.CompareTo("Y") == 0) payerDetails = true;
                if (showEFTDetails.CompareTo("Y") == 0) EFTDetails = true;
                if (showEFTTotal.CompareTo("Y") == 0) EFTTotal = true;

                sCmd.Dispose();

            }
            catch (Exception ex)
            {
                DetailLog += "? Error building member list" + NL;
                DetailLog += "  " + ex.Message.ToString() + NL;
                return;
            }
            if (Members.Count <= 0)
            {
                DetailLog += "? Subscriber " + SubscriberID + "has no Members" + NL;
                fError = true;
                return;
            }

           try
            {
                oCmd = OracleDbConnection.CreateCommand();
                oCmd.CommandText = "select r.pmt_id,to_char(r.pmt_amt,'$999,990.00') pmt_amt," +
                    "m.NCPDP_PRVDR_NUM,m.mbr_stor_nam " +
                    "from s_clm_pmt_rgstr r, s_ah_mbr m where " +
                    "r.mbr_id=m.mbr_id and r.mbr_id in ("+MbrList+") " +
                    "and trunc(r.pmt_dts) = to_date('"+pmtdate+"','mm/dd/yyyy') " +
                    "and r.pmt_stat_cd != 'V' " +
                    "order by r.pmt_id";
                oReader = oCmd.ExecuteReader();
                if (!oReader.HasRows)
                {
                    oReader.Close();
                    oReader.Dispose();
                    oCmd.Dispose();
                    return;
                }
                EFTSummary = "   " + "PMT".PadRight(12) + "NCPDP".PadRight(8) + "Amount".PadLeft(12) + " Store Name\n\n";
                while (oReader.Read())
                {
                    EFTSummary +=
                        "   " +
                        oReader["PMT_ID"].ToString().PadRight(12)+ 
                        oReader["NCPDP_PRVDR_NUM"].ToString().PadRight(8) +
                        oReader["PMT_AMT"].ToString().PadLeft(12) +
                        " " + oReader["MBR_STOR_NAM"] +
                        "\n";
                    EFTs.Add(oReader[0]);
                }

                oReader.Close();
                oReader.Dispose();
                
                // Get Grand Total if EFTTotal flag set
                //
                if (EFTTotal)
                {
                    oCmd.CommandText = "select to_char(Sum(r.pmt_amt),'$9,999,990.00') GrandTotal " +
                        "from s_clm_pmt_rgstr r where " +
                        "r.mbr_id in (" + MbrList + ") " +
                        "and trunc(r.pmt_dts) = to_date('" + pmtdate + "','mm/dd/yyyy') " +
                        "and r.pmt_stat_cd != 'V' ";
                    oReader = oCmd.ExecuteReader();
                    if (!oReader.HasRows)
                    {
                        oReader.Close();
                        oReader.Dispose();
                        oCmd.Dispose();
                        return;
                    }
                    EFTSummary += "\n";
                    while (oReader.Read())
                    {
                        EFTSummary +=
                            "   " + "Grand Total:".PadRight(18) +
                            oReader["GrandTotal"].ToString().PadLeft(14) +
                            "\n";
                    }

                    oReader.Close();
                    oReader.Dispose();
                }


		        // Get Payer info
                if (EFTs.Count > 0)
                {
                    for (int i = 0; i < EFTs.Count; i++)
                        if (i == 0) EFTList = "'" + EFTs[i].ToString() + "'";
                        else EFTList += ", '" + EFTs[i].ToString() + "'";

                    PayerList = "  Payers: \n";
                    if (!payerDetails)
                    {
                        oCmd.CommandText = "select plan_nam from s_ah_plan " +
                            " where ah_plan_id != 'AHADJ01' and ah_plan_id in (" +
                            " select distinct (ah_plan_id) from s_store_clm where pmt_id in (" + EFTList + ") " +
                            ")";
                        oReader = oCmd.ExecuteReader();

                        while (oReader.Read())
                        {
                            PayerList += "     " + oReader["PLAN_NAM"] + "\n";
                        }
                    }
                    else
                    {
                        oCmd.CommandText = "select p.PLAN_NAM,to_char(sum(aprv_claim_amt),'$999,990.00') PLAN_TOT "
                            + "from s_ah_plan p,s_store_clm c "
                            + "where c.ah_plan_id = p.ah_plan_id "
                            + "and c.pmt_id in (" + EFTList + ") "
                            + "group by p.PLAN_NAM order by p.PLAN_NAM";
                        oReader = oCmd.ExecuteReader();

                        while (oReader.Read())
                        {
                            PayerList += "     " + oReader["PLAN_NAM"].ToString().PadRight(26, ' ')
                                + oReader["PLAN_TOT"].ToString().PadLeft(10, ' ') + "\n";
                        }
                    }

                    oReader.Close();
                    oReader.Dispose();

                }
                oCmd.Dispose();

                AdvEmailNotice(SubscriberID);

            }
            catch (Exception ex)
            {
                DetailLog += "? Error Checking Payments" + NL;
                DetailLog += "  " + ex.Message.ToString() + NL;
                fError = true;
                return;
            }
           sCmd.Dispose();


        }
        static void AdvEmailNotice(string SubscriberID)
        {
            SqlCommand sCmd;
            SqlDataReader sReader;
            ArrayList EmailList = new ArrayList();
            string TemplateFilename;
            bool fHtml;

            try
            {
                sCmd = dbConnection.CreateCommand();
                sCmd.CommandText = "select t.TemplateFilename,t.ishtml from " +
                    "pmtreporting..Templates t, " +
                    "pmtreporting..EFTBrdcstSubs s " +
                    "where t.Class = s.SubClass ";
                sReader = sCmd.ExecuteReader();
                if (sReader.HasRows == false)
                {
                    sReader.Close();
                    sReader.Dispose();
                    sCmd.Dispose();
                    DetailLog += "? Subscriber " + SubscriberID + " has no matching email templates" + NL;
                    fError = true;
                    return;
                }
                sReader.Read();
                TemplateFilename = sReader["TemplateFilename"].ToString();
                string IsHtml = sReader["ISHTML"].ToString();
                fHtml = false;
                if (IsHtml.CompareTo("Y") == 0) fHtml = true;
                sReader.Close();
                sReader.Dispose();

                FileStream sTemp = new FileStream(TemplateFilename, FileMode.Open, FileAccess.Read);

                sCmd.CommandText = "select EmailAddr from pmtreporting..EFTBrdcstRcpts where SubscrID = '" +
                    SubscriberID + "'";
                sReader = sCmd.ExecuteReader();
                if (!sReader.HasRows)
                {
                    sReader.Close();
                    sReader.Dispose();
                    sCmd.Dispose();
                    DetailLog += "? Subscriber " + SubscriberID + " has no recipients" + NL;
                    fError = true;
                    return;
                }
                while (sReader.Read())
                {
                    EmailList.Add(sReader["EmailAddr"].ToString());
                    DetailLog += "Add Recipient: " + sReader["EmailAddr"].ToString() + NL;
                }
                sReader.Close();
                sReader.Dispose();
                sCmd.Dispose();

            }
            catch (Exception ex)
            {
                DetailLog += "? Error building Advanced Notice" + NL;
                DetailLog += "  " + ex.Message.ToString() + NL;
                fError = true;
                return;
            }

            SendNotice(SubscriberID, EmailList, TemplateFilename, fHtml); 

            DetailLog += "Send Advanced Notice for "+SubscriberID + NL;

            TotalAdvEmailsSent++;

        }
        static void SendNotice(string sID, ArrayList AddrList, string TemplateFilename, bool IsHTML)
        {

            string body;

            try
            {
                byte[] buffer;

                FileStream sTemp = new FileStream(TemplateFilename, FileMode.Open, FileAccess.Read);
                int length = (int)sTemp.Length;
                buffer = new byte[length];
                sTemp.Read(buffer, 0, length);

                body = System.Text.Encoding.UTF8.GetString(buffer);

                body = body.Replace("$$DATE", pmtdate);
                body = body.Replace("$$EFTLIST", EFTSummary);
                body = body.Replace("$$PAYERS", PayerList);

                DetailLog += "Template: " + TemplateFilename + NL;

            }
            catch (Exception ex)
            {
                DetailLog += "? Error building Notice" + NL;
                DetailLog += ex.Message.ToString() + NL;
                fError = true;
                return;
            }

            try
            {
                AHEmailMsg.AHEmailMsg msg = new AHEmailMsg.AHEmailMsg(DatabaseServer);
                msg.Body = body;
                msg.Subject = "Health Mart Atlas EFT completed";
                msg.From = "operations.hmatlas@mckesson.com";
                msg.ServiceID = sID;
                msg.Recipients = AddrList;
                msg.BodyIsHTML = IsHTML;
                msg.Send();
                DetailLog += "AHEmailMsg accepted" + NL;
                TotalEmailsSent++;
           }
            catch (Exception ex)
            {
                DetailLog += "? Error sending notice" + NL;
                DetailLog += ex.Message.ToString() + NL;
                fError = true;
                return;
            }


        }
        static void InternalEFTNotice()
        {
            OracleCommand oCmd = null;
            OracleDataReader oReader = null;
            bool fHTML=false;
            string DebitsList = "";
            //string ChkPayerList = "";
            //double TotalPaid = 0;
            //string TodaysTotalPaid = "";

            InternalTemplateFilename = "\\\\"+ TemplateServer + "\\templates\\InternalEFTNotice.txt";

            try
            {
                oCmd = OracleDbConnection.CreateCommand();
                oCmd.CommandText = "select m.mbr_id,m.ncpdp_prvdr_num," +
                    "r.pmt_id,to_char(r.pmt_amt,'$999,990.00') pmt_amt, " +
                    "m.mbr_stor_nam " +
                    "from s_clm_pmt_rgstr r, s_ah_mbr m where " +
                    "r.mbr_id=m.mbr_id and r.pmt_amt < 0" +
                    "and trunc(r.pmt_dts) = to_date('" + pmtdate + "','mm/dd/yyyy') " +
                    "and r.pmt_stat_cd != 'V' " +
                    "order by r.pmt_id";
                oReader = oCmd.ExecuteReader();

                DebitsList = "  Debits:" + NL;
                if (oReader.HasRows)
                {
                    DebitsList = "    " +
                        "Mbr ID".PadRight(8) +
                        "NCPDP".PadRight(8) +
                        "Pmt ID".PadRight(13) +
                        "Payment AMT".PadLeft(12) +
                        " Store Name" + NL;
                    while (oReader.Read())
                    {
                        DebitsList +=
                            "    " +
                            oReader["mbr_id"].ToString().PadRight(8) +
                            oReader["ncpdp_prvdr_num"].ToString().PadRight(8) +
                            oReader["PMT_ID"].ToString().PadRight(13) +
                            oReader["PMT_AMT"].ToString().PadLeft(12) +
                            " " + oReader["MBR_STOR_NAM"] +
                            NL;
                        
                    }
                }
                else
                {
                    DebitsList = "No debit transactions in Today's EFT";
                }
                oReader.Close();
                oReader.Dispose();


                // Build payer list for EFT payments
                oCmd.CommandText = "select p.plan_nam, to_char(sum(d.aprv_claim_amt),'$999,999,990.00') PLAN_PAID, " +
                    "sum(d.aprv_claim_amt) TOTALPLANPAID " +
                    "from s_ah_plan p, s_store_clm d, s_clm_pmt_rgstr r " +
                    "where trunc(r.pmt_dts) = to_date('" + pmtdate + "','mm/dd/yyyy') " +
                    "and r.pmt_type_cd = 'EFT' " +
                    "and p.ah_plan_id=d.ah_plan_id " +
                    "and d.AH_PLAN_ID != 'AHADJ001' " +
                    "and r.pmt_id = d.pmt_id " +
                    "and r.pmt_stat_cd != 'V' " +
                    " group by p.plan_nam order by p.plan_nam";
                oReader = oCmd.ExecuteReader();

                PayerList = "  Payers: \n";
                double limit = Convert.ToDouble(threshhold);
                while (oReader.Read())
                {
                    double pbmSum = oReader.GetDouble(oReader.GetOrdinal("TOTALPLANPAID"));
                    if ( pbmSum > limit)
                        PayerList += "     " + oReader["PLAN_NAM"].ToString().PadRight(30) +
                        oReader["PLAN_PAID"].ToString().PadLeft(15) + "\n";
                    //TotalPaid += oReader.GetDouble(oReader.GetOrdinal("TOTALPLANPAID"));
                }

                oReader.Close();
                oReader.Dispose();

                // Build payers for any Check payments
                /* Not wanted for now
                oCmd.CommandText = "select p.plan_nam, to_char(sum(d.aprv_claim_amt),'$999,999,990.00') PLAN_PAID, " +
                    "sum(d.aprv_claim_amt) TOTALPLANPAID " +
                    "from s_ah_plan p, s_store_clm d, s_clm_pmt_rgstr r " +
                    "where trunc(r.pmt_dts) = to_date('" + pmtdate + "','mm/dd/yyyy') " +
                    "and r.pmt_type_cd = 'CHK' " +
                    "and p.ah_plan_id=d.ah_plan_id " +
                    "and r.pmt_id = d.pmt_id " +
                    "and r.pmt_stat_cd != 'V' " +
                    "group by p.plan_nam order by p.plan_nam";
                oReader = oCmd.ExecuteReader();

                ChkPayerList = "  Check Payers: \n";
                while (oReader.Read())
                {
                    ChkPayerList += "     " + oReader["PLAN_NAM"].ToString().PadRight(30) +
                        oReader["PLAN_PAID"].ToString().PadLeft(15) + "\n";
                    TotalPaid += oReader.GetDouble(oReader.GetOrdinal("TOTALPLANPAID"));
                }

                oReader.Close();
                oReader.Dispose();
                 * */

                //TodaysTotalPaid = "Today's Total Payments: " + TotalPaid.ToString("$###,###,##0.00");
                
                oCmd.Dispose();
            }
            catch (Exception ex)
            {
                DetailLog += "? Error generating internal EFT Notice" + NL;
                DetailLog += "  " + ex.Message.ToString() + NL;
                fError = true;
                return;
            }

            string body;



            try
            {
                byte[] buffer;

                FileStream sTemp = new FileStream(InternalTemplateFilename, FileMode.Open, FileAccess.Read);
                int length = (int)sTemp.Length;
                buffer = new byte[length];
                sTemp.Read(buffer, 0, length);

                body = System.Text.Encoding.UTF8.GetString(buffer);

                body = body.Replace("$$DATE", pmtdate);
                body = body.Replace("$$DEBITS", DebitsList);
                body = body.Replace("$$PAYERS", PayerList);
                //body = body.Replace("$$CHECKPAYERS", ChkPayerList);
                //body = body.Replace("$$TOTALPAID", TodaysTotalPaid);

                DetailLog += "Template: " + InternalTemplateFilename + NL;

            }
            catch (Exception ex)
            {
                DetailLog += "? Error building Internal Notice" + NL;
                DetailLog += ex.Message.ToString() + NL;
                fError = true;
                return;
            }


            //MailMessage msg = new MailMessage();
            AHEmailMsg.AHEmailMsg msg = new AHEmailMsg.AHEmailMsg(DatabaseServer);
            msg.Body = body;
            msg.Subject = "Health Mart Atlas EFT completed";
            msg.From = "operations.hmatlas@mckesson.com";
            ArrayList rcptsList = new ArrayList();
            SqlCommand sCmd = null;
            SqlDataReader sReader = null;
            ArrayList IntRcptList = new ArrayList();
            try
            {
                sCmd = dbConnection.CreateCommand();

                sCmd.CommandText = "select EmailAddress from pmtreporting..EFTBrdcstIntRcpts order by EmailAddress";
                sReader = sCmd.ExecuteReader();
                if (!sReader.HasRows)
                {
                    sReader.Close();
                    sReader.Dispose();
                    sCmd.Dispose();
                    DetailLog += "? No Internal Recipients found" + NL;
                    fError = true;
                    return;
                }
                while (sReader.Read())
                {
                    IntRcptList.Add(sReader["EmailAddress"].ToString());
                    DetailLog += "Add Recipient: " + sReader["EmailAddress"].ToString() + NL;
                }
                sReader.Close();
                sReader.Dispose();
                sCmd.Dispose();

            }
            catch (Exception ex)
            {
                DetailLog += "? Error getting 'TO' list for internal notice " + NL;
                DetailLog += ex.Message.ToString() + NL;
                if (sReader != null) sReader.Close();
                if (sCmd != null) sCmd.Dispose();

                fError = true;
                return;
            }
            //msg.IsBodyHtml = fHTML;
            //SmtpClient client = new SmtpClient(SMTPServer);
            //client.Send(msg);
            try
            {
                msg.BodyIsHTML = fHTML;
                msg.Recipients = IntRcptList;
                msg.Send();
                DetailLog += "Internal EFT Notice Sent" + NL;            }
            catch (Exception ex)
            {
                DetailLog += "? Error sending internal notice." + NL;
                DetailLog += ex.Message.ToString() + NL;
                fError = true;
            }



        }
        static int LaunchAHMailer()
        {
            int iCount = 0;

            //Only launch the mailer if we're running again default server
            //
            if (!DatabaseServer.Equals(DefaultServer)) return 0;

            try
            {
                SqlCommand cList = new SqlCommand("insert into AHSystems..TaskQueue (TargetServer,TaskName,TimeQueued,Status,QueuedBy) "
                    + "values ('MCKPH-016', 'LaunchAHMailer','" + DateTime.Now.ToString() + "', 'P', 'EFTBrdcst')",
                    dbConnection);
                iCount = cList.ExecuteNonQuery();
                cList.Dispose();
            }
            catch (Exception ex)
            {
                DetailLog += "SQL Error" + "Queue AHMailer failed, exception: " + ex.Message.ToString();
                return 0;
            }

            DetailLog += "Queue AHMailer succeeded, count(" + iCount.ToString() + ")";

            return iCount;
        }

        static void OutputRunSummary()
        {
            string body="";

            try
            {
                StreamWriter sLog = new StreamWriter("\\\\" + TemplateServer + "\\Logs\\EFTBrdcst.log", false);
                sLog.Write(body);
                sLog.WriteLine("");
                sLog.WriteLine("Details:");
                sLog.WriteLine("========");
                sLog.Write(DetailLog);
                sLog.Close();
                sLog.Dispose();

            }
            catch (Exception mex)
            {
                Console.WriteLine("LOGFAIL" + mex.ToString());
            }
            
            if (DebugFlag)
            {
                Console.WriteLine("Detail Log: ");
                Console.Write(DetailLog);
            }

            try
            {
                //MailMessage msg = new MailMessage();
                AHEmailMsg.AHEmailMsg msg = new AHEmailMsg.AHEmailMsg(DatabaseServer);
                msg.From = "operations.hmatlas@mckesson.com";
                // If production systems, set operations to notice else developer
                msg.Recipients = new ArrayList();
                if (DatabaseServer.ToUpper().CompareTo(DefaultServer.ToUpper()) == 0)
                {
                    msg.Recipients.Add("operations.hmatlas@mckesson.com");
                    if (fError) msg.Recipients.Add("#DLAHOpsTech@mckesson.com");
                }
                else
                    msg.Recipients.Add("#DLAHOpsTech@mckesson.com");
                msg.Subject = "EFTBrdcst Summary results";

                if (fError) body += "EFTBrdcst complete with errors!!" + NL + NL;

                body += "EFTBroadcast summary("+pmtdate+"):" + NL +
                        "  Basic Subsribers: " + TotalBasicSubscribers.ToString() + NL +
                        "  Advanced Subscribers: " + TotalAdvancedSubscribers.ToString() + NL +
                        "  Basic Notices sent: " + TotalBasicEmailsSent.ToString() + NL +
                        "  Advanced Notices sent: " + TotalAdvEmailsSent.ToString() + NL +
                        "  Notifications Sent: " + TotalEmailsSent.ToString() + NL +
                        "  Internal Notice Recipient Count: " + TotalInternalNoticeRcpts.ToString() + NL;

                msg.Body = body;
                msg.BodyIsHTML = false;
                //SmtpClient client = new SmtpClient(SMTPServer);
                //client.Send(msg);
                msg.Send();
            }
            catch (Exception mex)
            {
                Console.WriteLine("MRPT01"+mex.ToString());
                //Try direct send for error condition as last resort.
                //
                MailMessage SMTPmsg = new MailMessage();
                SMTPmsg.From = new MailAddress("operations.hmatlas@mckesson.com");
                SMTPmsg.To.Add("#DLAHOpsTech@mckesson.com");
                SMTPmsg.Subject = "ERROR Results: EFTBrdcst Summary results";
                SMTPmsg.Body = body;
                SMTPmsg.IsBodyHtml = false;
                SmtpClient client = new SmtpClient(SMTPServer);
                client.Send(SMTPmsg);
            }

        }

        private static Dictionary<string, string> getDBConfig()
        {
            string iniFile = "EFTBrdcst.ini";
           // string iniFile = "C:\\GitHub\\eftbrdcst\\EFTBrdcst.ini"; //local testing
            Dictionary<string, string> dbConfig = new Dictionary<string, string>();
            try
            {
                StreamReader fsIn = new StreamReader(iniFile);
                string _line;
                while ((_line = fsIn.ReadLine()) != null)
                {
                    string[] keyvalue = _line.Split('=');
                    if (keyvalue.Length == 2)
                    {
                        dbConfig.Add(keyvalue[0], keyvalue[1]);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message.ToString());

            }
            return dbConfig;
        }

    }


}
