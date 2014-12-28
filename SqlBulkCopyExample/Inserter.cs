using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using Dapper;
using System.Dynamic;

namespace SqlBulkCopyExample
{
    public interface IInserter<T>
    {
        string TableName { get; }

        T AfterInsert(T item, IDictionary<string, object> identities);

        IEnumerable<T> Insert(
            IEnumerable<T> items,
            IDbConnection conn,
            IDbTransaction externalTransaction = null,
            Action<IEnumerable<T>, IDbConnection, IDbTransaction> beforeCommit = null);
    }

    public abstract class BaseInserter<T> : IInserter<T>
    {
        public string TableName { get; private set; }

        private readonly List<ColumnMapping<T>> Mappings;

        public BaseInserter(string tableName)
        {
            TableName = tableName;
            Mappings = new List<ColumnMapping<T>> { };
        }

        public abstract T AfterInsert(T item, IDictionary<string, object> identities);

        protected virtual void BeforeCommit(IEnumerable<T> items, IDbConnection conn, IDbTransaction currentTransaction)
        {
            return;
        }
        
        public IEnumerable<T> Insert(
            IEnumerable<T> items, 
            IDbConnection conn, 
            IDbTransaction externalTransaction = null, 
            Action<IEnumerable<T>, IDbConnection, IDbTransaction> beforeCommit = null)
        {
            if (items == null || items.Any() == false)
                return items;
            
            var columns = Mappings.Where(x => !x.IsIdentity);
            var identities = Mappings.Where(x => x.IsIdentity);

            Action<IDbTransaction> insert = transaction =>
            {
                if (identities.Any())
                    items = ExecuteInsert(items, columns, identities, conn, transaction);
                else
                    ExecuteInsert(items, columns, conn, transaction);

                BeforeCommit(items, conn, transaction);

                if (beforeCommit != null)
                    beforeCommit(items, conn, transaction);
            };

            if (externalTransaction == null)
            {
                using (var transaction = conn.BeginTransaction())
                {
                    insert(transaction);

                    transaction.Commit();
                }
            }
            else
            {
                if (externalTransaction.Connection != conn)
                    throw new InvalidOperationException("The transaction was started by a different connection");

                insert(externalTransaction);
            }

            return items;
        }

        protected virtual int ExecuteInsert(IEnumerable<T> items, IEnumerable<ColumnMapping<T>> columns, IDbConnection conn, IDbTransaction transaction)
        {
            var insert = String.Format("{0} {1} {2};", BeginInsertStatement(), ListColumns(columns), ListValues(columns));
            var result = 0;
            var columnList = columns.ToList();
            var transformedItems = items.Select(x =>
            {
                var expando = new ExpandoObject { };
                columnList.ForEach(y => y.MapToRow(expando, x));
                return expando;
            })
            .ToList();

            result = conn.Execute(insert, transformedItems, transaction);

            return result;
        }

        protected virtual IEnumerable<T> ExecuteInsert(
            IEnumerable<T> items, 
            IEnumerable<ColumnMapping<T>> columns,
            IEnumerable<ColumnMapping<T>> identities,
            IDbConnection conn, 
            IDbTransaction transaction)
        {
            var rng = new Random(Environment.TickCount);
            var tempTable = String.Format("{0}_{1}", TableName, rng.Next(0, 10000000));
            var insertStatement = String.Format("{0} {1} {2} {3};", 
                BeginInsertStatement(),
                ListColumns(columns),
                InsertIntoTempTable(tempTable, identities),
                ListValues(columns));
            var columnList = columns.ToList();
            var transformedItems = items.Select(x =>
            {
                var expando = new ExpandoObject { };
                columnList.ForEach(y => y.MapToRow(expando, x));
                return expando;
            })
           .ToList();

            conn.Execute(CreateTempTable(tempTable, identities), null, transaction);

            conn.Execute(insertStatement, transformedItems, transaction);

            var results = conn.Query(SelectTempTable(tempTable), null, transaction).Select(d => d as IDictionary<string, object>).ToArray();

            if (results == null || results.Any() == false)
                throw new DataException("Failed to retrieve any results from the INSERT");

            if (results.Count() != items.Count())
                throw new DataException("Received the incorrect number of results from the INSERT");
                
            items = items.Select((x, idx)
                => AfterInsert(x, results[idx]))
                .ToList();

            conn.Execute(DropTempTable(tempTable), null, transaction);
            
            return items;
        }

        protected virtual string CreateTempTable(string name, IEnumerable<ColumnMapping<T>> identities)
        {
            var r= String.Format(@"
                IF OBJECT_ID('tempdb..#{0}') IS NOT NULL 
                    BEGIN 
                        DROP TABLE #{0} 
                    END;               
                CREATE TABLE #{0} ({1});", 
                name,
                identities.ToString(x => x.DbColumnName + " " + x.DbType));

            return r;
        }

        protected virtual string BeginInsertStatement()
        {
            return String.Format("INSERT INTO {0}", TableName);
        }

        protected virtual string ListColumns(IEnumerable<ColumnMapping<T>> columns)
        {
            return String.Format("({0})", columns.ToString(x => x.DbColumnName));
        }

        protected virtual string InsertIntoTempTable(string name, IEnumerable<ColumnMapping<T>> identities)
        {
            return String.Format("OUTPUT {0} INTO #{1} ({2})",
                identities.ToString(x => "INSERTED." + x.DbColumnName),
                name,
                identities.ToString(x => x.DbColumnName));
        }

        protected virtual string ListValues(IEnumerable<ColumnMapping<T>> columns)
        {
            return String.Format("VALUES ({0})", columns.ToString(x => "@" + x.DbColumnName));
        }

        protected virtual string SelectTempTable(string name)
        {
            return String.Format("SELECT * FROM #{0};", name);
        }

        protected virtual string DropTempTable(string name)
        {
            return String.Format("DROP TABLE #{0};", name);
        }
        
        protected ColumnMapping<T> Column<TSelector>(string dbColumn, Func<T, TSelector> map)
        {
            Action<IDictionary<string, object>, T> mapToRow = (d, t) => d[dbColumn] = map(t);

            var mapping = new ColumnMapping<T>(dbColumn, mapToRow, isIdentity: false, dbType: null);

            Mappings.Add(mapping);

            return mapping;
        }

        protected ColumnMapping<T> Column<TProperty>(Expression<Func<T, TProperty>> dbColumn)
        {
            var property = GetPropertyInfo(dbColumn);

            return Column(property.Name, dbColumn.Compile());
        }

        protected ColumnMapping<T> Identity(string dbColumn, string dbType)
        {
            var mapping = new ColumnMapping<T>(dbColumn, mapToRow: null, isIdentity: true, dbType: dbType);

            Mappings.Add(mapping);

            return mapping;
        }

        protected ColumnMapping<T> Identity<TProperty>(Expression<Func<T, TProperty>> dbColumn, string dbType)
        {
            var property = GetPropertyInfo(dbColumn);

            return Identity(property.Name, dbType);
        }

        private static PropertyInfo GetPropertyInfo<TProperty>(Expression<Func<T, TProperty>> propertyLambda)
        {
            var type = typeof(T);

            var member = propertyLambda.Body as MemberExpression;
            if (member == null)
                throw new ArgumentException(string.Format(
                    "Expression '{0}' refers to a method, not a property.",
                    propertyLambda.ToString()));

            var propInfo = member.Member as PropertyInfo;
            if (propInfo == null)
                throw new ArgumentException(string.Format(
                    "Expression '{0}' refers to a field, not a property.",
                    propertyLambda.ToString()));

            if (type != propInfo.ReflectedType &&
                !type.IsSubclassOf(propInfo.ReflectedType))
                throw new ArgumentException(string.Format(
                    "Expresion '{0}' refers to a property that is not from type {1}.",
                    propertyLambda.ToString(),
                    type));

            return propInfo;
        }
    }

    public class ColumnMapping<T>
    {
        public string DbColumnName { get; private set; }

        public bool IsIdentity { get; private set; }

        public string DbType { get; private set; }

        public Action<IDictionary<string, object>, T> MapToRow { get; private set; }

        public ColumnMapping(string dbColumnName, Action<IDictionary<string, object>, T> mapToRow = null, bool isIdentity = false, string dbType = null)
        {
            DbColumnName = dbColumnName;
            MapToRow = mapToRow;
            IsIdentity = isIdentity;
            DbType = dbType;
        }
    }

    public static class ColumnMappingExt
    {
        public static string ToString<T>(this IEnumerable<ColumnMapping<T>> items, Func<ColumnMapping<T>, string> format, string joiner = ", ")
        {
            if (items == null)
                return null;

            return String.Join(joiner, items.Select(format).ToArray());
        }
    }
}
