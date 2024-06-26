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
            int numberOfPartitions = 10;
            List<DataTable> partitions = PartitionDataTable(dataTable, numberOfPartitions);

            // Execute operations on each partition in parallel
            Parallel.ForEach(partitions, (partition, state, index) =>
            {
                ProcessPartition(partition,index);
            });

            Console.Write("Processing complete.");
        }

        static List<DataTable> PartitionDataTableFixedSize(DataTable dt, int partitionSize)
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

        static List<DataTable> PartitionDataTable(DataTable dt, int numberOfPartitions)
        {
            List<DataTable> partitions = new List<DataTable>();
            int totalRows = dt.Rows.Count;
            int partitionSize = (totalRows + numberOfPartitions - 1) / numberOfPartitions; // Ceiling division to ensure all rows are included

            for (int i = 0; i < numberOfPartitions; i++)
            {
                DataTable partition = dt.Clone(); // Clone the structure of the DataTable
                int start = i * partitionSize;
                int end = Math.Min(start + partitionSize, totalRows);

                for (int j = start; j < end; j++)
                {
                    partition.ImportRow(dt.Rows[j]);
                }

                Console.WriteLine($"Partition {i + 1} created with {partition.Rows.Count} rows.");
                partitions.Add(partition);
            }

            return partitions;
        }

        static void ProcessPartition(DataTable partition, long partitionIndex)
        {
            Console.WriteLine($"Processing partition {partitionIndex + 1} with {partition.Rows.Count} rows.");

            foreach (DataRow row in partition.Rows)
            {
                // Log the current record being processed
                Console.WriteLine($"Partition {partitionIndex + 1}: Processing record Id={row["Id"]}");

                // Example operation: Update a column value
                row["Name"] = row["Name"] + "_processed";

                // Simulate some processing time
                System.Threading.Thread.Sleep(10); // For example, simulate some time-consuming processing
            }

            Console.WriteLine($"Finished processing partition {partitionIndex + 1}.");
        }       

        public static DataTable GetRecordsToProcess()
        {
            DataTable dataTable = new DataTable();
            dataTable.Columns.Add("Id", typeof(int));
            dataTable.Columns.Add("Name", typeof(string));

            // Adding sample data
            for (int i = 0; i < 10000; i++)
            {
                dataTable.Rows.Add(i, "Name" + i);
            }
            return dataTable;
        }

    }
}
