namespace SqlBulkCopyExample
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.SqlClient;
    using System.Diagnostics;
    using System.Linq;
    using SqlBulkCopyExample.Properties;
    using Dapper;
    using System.Dynamic;

    class Program
    {
        static void Main(string[] args)
        {
            var people = CreateSamplePeople(10000);

            using (var connection = new SqlConnection("Server=(local);Database=Labs;Integrated Security=SSPI"))
            {
                connection.Open();

                var stopwatch = new Stopwatch();

                // ------ SQL Bulk Insert
                // Warm up...
                RecreateDatabase(connection);
                InsertDataUsingSqlBulkCopy(people, connection);

                // Measure
                stopwatch.Start();
                InsertDataUsingSqlBulkCopy(people, connection);
                Console.WriteLine("Bulk copy: {0}ms", stopwatch.ElapsedMilliseconds);

                // ------ Inserter + Dapper
                // Warm up...
                RecreateDatabase(connection);
                InsertDataUsingInserter(people, connection);

                // Measure
                stopwatch.Reset();
                stopwatch.Start();
                InsertDataUsingInserter(people, connection);
                Console.WriteLine("Inserter + Dapper: {0}ms", stopwatch.ElapsedMilliseconds);

                // ------ Inserter + Dapper 2
                // Warm up...
                RecreateDatabase(connection);
                InsertDataUsingInserter2(people, connection);

                // Measure
                stopwatch.Reset();
                stopwatch.Start();
                InsertDataUsingInserter2(people, connection);
                Console.WriteLine("Inserter + Dapper 2: {0}ms", stopwatch.ElapsedMilliseconds);

                // ------ Inserter + Dapper 3
                // Warm up...
                RecreateDatabase(connection);
                InsertDataUsingInserter3(people, connection);

                // Measure
                stopwatch.Reset();
                stopwatch.Start();
                InsertDataUsingInserter3(people, connection);
                Console.WriteLine("Inserter + Dapper 3: {0}ms", stopwatch.ElapsedMilliseconds);

                // ------ Insert statements
                // Warm up...
                RecreateDatabase(connection);
                InsertDataUsingInsertStatements(people, connection);

                // Measure
                stopwatch.Reset();
                stopwatch.Start();
                InsertDataUsingInsertStatements(people, connection);
                Console.WriteLine("Individual insert statements: {0}ms", stopwatch.ElapsedMilliseconds);

                Console.ReadKey();
            }
        }

        private static void InsertDataUsingInsertStatements(IEnumerable<Person> people, SqlConnection connection)
        {
            //using (var command = connection.CreateCommand())
            //{
            //    command.CommandText = "INSERT INTO Person (Name, DateOfBirth) VALUES (@Name, @DateOfBirth)";
            //    var nameParam = command.Parameters.Add("@Name", SqlDbType.NVarChar);
            //    var dobParam = command.Parameters.Add("@DateOfBirth", SqlDbType.DateTime);
            //    foreach (var person in people)
            //    {
            //        nameParam.Value = person.Name;
            //        dobParam.Value = person.DateOfBirth;
            //        command.ExecuteNonQuery();
            //    }
            //}
        }

        private static void InsertDataUsingSqlBulkCopy(IEnumerable<Person> people, SqlConnection connection)
        {
            //var bulkCopy = new SqlBulkCopy(connection);
            //bulkCopy.DestinationTableName = "Person";
            //bulkCopy.ColumnMappings.Add("Name", "Name");
            //bulkCopy.ColumnMappings.Add("DateOfBirth", "DateOfBirth");

            //using (var dataReader = new ObjectDataReader<Person>(people))
            //{
            //    bulkCopy.WriteToServer(dataReader);
            //}
        }

        private static void InsertDataUsingInserter(IEnumerable<Person> people, SqlConnection connection)
        {
            IInserter<Person> inserter = new PersonInserter();

            var newPeople = inserter.Insert(people, connection, beforeCommit: (items, conn, trans) =>
            {
                var kids = items.SelectMany(p => p.Kids.Select(k => new { p.PersonId, k.Age })).ToList();

                var insertKids = "INSERT INTO Kid (PersonId, Age) VALUES (@PersonId, @Age)";

                var rowsInserted = conn.Execute(insertKids, kids, trans);

                if (rowsInserted != kids.Count())
                    throw new Exception("Did not insert the correct number of kids");
            });
        }

        private static void InsertDataUsingInserter2(IEnumerable<Person> people, SqlConnection connection)
        {
            IInserter<Person> peopleInserter = new PersonInserter();

            people = peopleInserter.Insert(people, connection, beforeCommit: (items, conn, trans) =>
            {
                IInserter<Tuple<int, Kid>> kidsInserter = new KidInserter();

                var kids = items.SelectMany(p => p.Kids.Select(k => new Tuple<int, Kid>(p.PersonId, k)));

                kidsInserter.Insert(kids, conn, trans);
            });
        }

        private static void InsertDataUsingInserter3(IEnumerable<Person> people, SqlConnection connection)
        {
            IInserter<Person> peopleInserter = new PersonInserter();

            people = peopleInserter.Insert(people, connection, beforeCommit: (items, conn, trans) =>
            {
                IInserter<PersonToKidMapping> kidsInserter = new KidInserter2();

                var kids = items.SelectMany(p => p.Kids.Select(k => new PersonToKidMapping(p, k) ));

                kidsInserter.Insert(kids, conn, trans);
            });
        }

        private static void RecreateDatabase(SqlConnection connection)
        {
            using (var command = connection.CreateCommand())
            {
                command.CommandText = Resources.CreateDatabase;
                command.CommandType = CommandType.Text;
                command.ExecuteNonQuery();
            }
        }

        private static IEnumerable<Person> CreateSamplePeople(int count)
        {
            return Enumerable
                .Range(0, count)
                .Select(i => {
                    return new Person
                    {
                        Name = "Person" + i,
                        DateOfBirth = new DateTime(1950 + (i % 50), ((i * 3) % 12) + 1, ((i * 7) % 29) + 1),
                        PhoneNumber = new PhoneNumber { AreaCode = "123", Number = "12345678" },
                        Kids = new List<Kid> { new Kid { Age = 3 } }
                    };
                });
        }
    }

}
