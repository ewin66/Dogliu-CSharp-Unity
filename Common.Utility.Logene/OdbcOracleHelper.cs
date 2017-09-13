using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Odbc;

namespace Common.Utility.Logene
{
    public class OdbcOracleHelper
    {
        /// <summary>
        /// 通过ODBC查询DataTable
        /// </summary>
        /// <param name="strConn">连接串</param>
        /// <param name="sql">sql语句</param>
        /// <returns></returns>
        public static DataTable GetTable(string strConn, string sql)
        {
            OdbcConnection odbcconn = new OdbcConnection(strConn);

            try
            {
                odbcconn.Open();
                string strComm = sql;
                OdbcDataAdapter odbcda = new OdbcDataAdapter(strComm, odbcconn);
                DataSet ds = new DataSet();
                odbcda.Fill(ds);
                DataTable dt = new DataTable();
                dt = ds.Tables[0];
                return dt;
            }
            catch (Exception e)
            {
                throw;
            }
            finally
            {
                odbcconn.Close();
                odbcconn.Dispose();
            }
        }

        public static void ExecuteNonQuery(string strConn, string sql)
        {
            OdbcConnection odbcconn = new OdbcConnection(strConn);
            try
            {
                odbcconn.Open();
                string strComm = sql;
                OdbcCommand cmd = new OdbcCommand(strComm);
                cmd.Connection = odbcconn;
                cmd.ExecuteNonQuery();
            }
            catch (Exception e)
            {
                throw;
            }
            finally
            {
                odbcconn.Close();
                odbcconn.Dispose();
            }
        }

        public static void ExecuteBatch(string connStr, List<string> lstSql)
        {
            OdbcConnection odbcconn = new OdbcConnection(connStr);
            OdbcCommand cmd = new OdbcCommand();
            OdbcTransaction trans = null;
            try
            {
                odbcconn.Open();
                trans = odbcconn.BeginTransaction();

                cmd.Connection = odbcconn;
                cmd.Transaction = trans;

                foreach (string sql in lstSql)
                {
                    string strComm = sql;
#if DEBUG
                    Console.WriteLine("exec batch:"+strComm);
#endif
                    cmd.CommandText = sql;
                    cmd.ExecuteNonQuery();
                }
                trans.Commit();
            }
            catch (Exception e)
            {
                trans?.Rollback();
                throw;
            }
            finally
            {
                odbcconn.Close();
                odbcconn.Dispose();
            }
        }

        public static void ExecuteBatch(string connStr, List<MySqlCommand> lstSql)
        {
            OdbcConnection odbcconn = new OdbcConnection(connStr);
            OdbcCommand cmd = new OdbcCommand();
            OdbcTransaction trans = null;
            try
            {
                odbcconn.Open();
                trans = odbcconn.BeginTransaction();

                cmd.Connection = odbcconn;
                cmd.Transaction = trans;

                foreach (MySqlCommand mySqlCommand in lstSql)
                {
                    string strComm = mySqlCommand.SqlText;
#if DEBUG
                    Console.WriteLine("exec batch:" + strComm);
#endif
                    cmd.CommandText = strComm;
                    cmd.Parameters.Clear();
                    cmd.Parameters.AddRange(mySqlCommand.Parameters.ToArray());

                    cmd.ExecuteNonQuery();
                }
                trans.Commit();
            }
            catch (Exception e)
            {
                trans?.Rollback();
                throw;
            }
            finally
            {
                odbcconn.Close();
                odbcconn.Dispose();
            }
        }

        /// <summary>
        /// sql入参防注入处理,把敏感字替换成chr(*)的形式
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        public static string GetSafeSql(string sql)
        {
            sql = sql.Replace(",", "'||chr(39)||'");
            return sql;
        }
    }

    public class MySqlCommand
    {
        public MySqlCommand() { }

        public MySqlCommand(string sqlText, List<OdbcParameter> parameters)
        {
            SqlText = sqlText;
            Parameters = parameters;
        }

        public string SqlText { get; set; }
        public List<OdbcParameter> Parameters { get; set; } = new List<OdbcParameter>();


    }


}