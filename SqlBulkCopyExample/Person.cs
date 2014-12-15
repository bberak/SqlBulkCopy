using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace SqlBulkCopyExample
{
    public class Person
    {
        public int PersonId { get; set; }
        public string Name { get; set; }
        public DateTime? DateOfBirth { get; set; }
        public List<Kid> Kids { get; set; }
        public PhoneNumber PhoneNumber { get; set; }
    }

    public class Kid
    {
        public int KidId { get; set; }
        public int Age { get; set; }
    }

    public class PhoneNumber
    {
        public string AreaCode { get; set; }
        public string Number { get; set; }
    }

    public class PersonInserter : BaseInserter<Person>
    {
        public PersonInserter()
            : base("Person")
        {
            Column("PersonId", dbType: "INT");
            Column("Name", x => x.Name);
            Column("DateOfBirth", x => x.DateOfBirth);
            Column("AreaCode", x => x.PhoneNumber.AreaCode);
            Column("Number", x => x.PhoneNumber.Number);
            Column("FullPhoneNumber", x => x.PhoneNumber.AreaCode  + "-" + x.PhoneNumber.Number);
            Column("HasKids", x => x.Kids.Any());
            Column("SumOfKidsAge", x => x.Kids.Sum(y => y.Age));
        }

        public override Person AfterAutoValuesRetrieved(Person src, IDictionary<string, object> autoValues)
        {
            src.PersonId = (int)autoValues["PersonId"];

            return src;
        }
    }
}
