using System;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;

namespace SqlBulkCopysd
{
    public class Program
    {
        public static void BulkToDB(DataTable dt)
        {
            SqlConnection sqlConn = new SqlConnection(ConfigurationManager.ConnectionStrings["conStr"].ConnectionString);
            SqlBulkCopy bulkCopy = new SqlBulkCopy(sqlConn);

            bulkCopy.DestinationTableName = "BulkTestTable";
            bulkCopy.BatchSize = dt.Rows.Count;
            Console.WriteLine("{0}", dt.Rows.Count);
            try
            {
                sqlConn.Open();
                if (dt != null && dt.Rows.Count != 0)
                    bulkCopy.WriteToServer(dt);
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                sqlConn.Close();
                if (bulkCopy != null)
                    bulkCopy.Close();
            }
        }

        public static DataTable GetTableSchema()
        {
            DataTable dt = new DataTable();
            dt.Columns.AddRange(new DataColumn[]{
            new DataColumn("Id",typeof(int)),
            new DataColumn("UserName",typeof(string)),
            new DataColumn("Pwd",typeof(string))});
            return dt;
        }

        static void Main(string[] args)
        {
            Stopwatch sw = new Stopwatch();
            DateTime beforDT = System.DateTime.Now;
            for (int multiply = 1; multiply < 101; multiply++)
            {
                DataTable dt = GetTableSchema();
                for (int count = multiply * 1000000; count < (multiply + 1) * 1000000; count++)
                {
                    DataRow r = dt.NewRow();
                    r[0] = count;
                    r[1] = string.Format("User-{0}", count * multiply);
                    r[2] = string.Format("Pwd-{0}", count * multiply);
                    dt.Rows.Add(r);
                }
                sw.Start();
                BulkToDB(dt);
                sw.Stop();
                Console.WriteLine(string.Format("Elapsed Time is {0} Milliseconds", sw.ElapsedMilliseconds));
            }
            DateTime afterDT = System.DateTime.Now;
            TimeSpan ts = afterDT.Subtract(beforDT);
            Console.WriteLine(ts.TotalMilliseconds.ToString());
            Console.ReadLine();
        }
    }
}
