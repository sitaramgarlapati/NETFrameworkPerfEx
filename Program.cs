using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NETFrameworkPerfEx
{
    class Program
    {
        static void Main(string[] args)
        {
            ProcessRecordsInParallel();

        }

        public static void ProcessRecordsInParallel()
        {
            DataTable dataTable = GetRecordsToProcess();
            const int batchSize = 100; // Adjust batch size as necessary

            // Process records in parallel batches
            Parallel.ForEach(PartitionDataTable(dataTable, batchSize), batch =>
            {
                List<int> processedRecordIds = new List<int>();
                foreach (DataRow row in batch)
                {
                    ProcessRecord(row);
                    processedRecordIds.Add(Convert.ToInt32(row["Id"]));
                }

            });
        }
               
        public static  IEnumerable<DataRow[]> PartitionDataTable(DataTable table, int batchSize)
        {
            for (int i = 0; i < table.Rows.Count; i += batchSize)
            {
                DataRow[] batch = new DataRow[Math.Min(batchSize, table.Rows.Count - i)];
                table.Rows.CopyTo(batch, i);
                yield return batch;
            }
        }

        public static DataTable GetRecordsToProcess()
        {
            DataTable dataTable = new DataTable();
            using (SqlConnection conn = new SqlConnection("YourConnectionString"))
            {
                conn.Open();
                using (SqlCommand cmd = new SqlCommand("SELECT Id, ... FROM YourTable WHERE Status = 'Pending'", conn))
                {
                    using (SqlDataAdapter adapter = new SqlDataAdapter(cmd))
                    {
                        adapter.Fill(dataTable);
                    }
                }
            }
            return dataTable;
        }

        public static void ProcessRecord(DataRow row)
        {
            // Your processing logic here
        }

        public class Record
        {
            public int Id { get; set; }
            // Add other properties as needed
        }



    }
}
