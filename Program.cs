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

            // Process records in parallel batches
            // Partition the DataTable
            int partitionSize = 1000;
            List<DataTable> partitions = PartitionDataTable(dataTable, partitionSize);

            // Execute operations on each partition in parallel
            Parallel.ForEach(partitions, partition =>
            {
                ProcessPartition(partition);
            });

            Console.Write("Processing complete.");
        }

        static List<DataTable> PartitionDataTable(DataTable dt, int partitionSize)
        {
            List<DataTable> partitions = new List<DataTable>();
            int totalRows = dt.Rows.Count;
            int totalPartitions = (totalRows + partitionSize - 1) / partitionSize; // Ceiling division

            Console.WriteLine("Number of Partitions: " + totalPartitions);

            for (int i = 0; i < totalPartitions; i++)
            {
                DataTable partition = dt.Clone(); // Clone the structure of the DataTable
                int start = i * partitionSize;
                int end = Math.Min(start + partitionSize, totalRows);

                for (int j = start; j < end; j++)
                {
                    partition.ImportRow(dt.Rows[j]);
                }

                partitions.Add(partition);
            }

            return partitions;
        }

        static void ProcessPartition(DataTable partition)
        {
            // Example operation: Print the row count of the partition
            Console.WriteLine("Processing partition with row count: " + partition.Rows.Count);

            // Example operation: Update a column value
            foreach (DataRow row in partition.Rows)
            {
                row["Name"] = row["Name"] + "_processed";
            }

            // Simulate some processing time
            System.Threading.Thread.Sleep(100); // For example, simulate some time-consuming processing
        }

        public static DataTable GetRecordsToProcess()
        {
            DataTable dataTable = new DataTable();
            dataTable.Columns.Add("Id", typeof(int));
            dataTable.Columns.Add("Name", typeof(string));

            // Adding sample data
            for (int i = 0; i < 2500; i++)
            {
                dataTable.Rows.Add(i, "Name" + i);
            }
            return dataTable;
        }

    }
}
