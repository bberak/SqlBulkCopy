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

    public class PersonInserter : BaseInserter<Person, int>
    {
        public PersonInserter()
            : base("Person")
        {
            Auto(x => x.PersonId, "INT");
            Column(x => x.Name);
            Column(x => x.DateOfBirth);
            Column(x => x.PhoneNumber.Number);
        }

        public override int ConvertToAutoValue(IDictionary<string, object> dbKeys)
        {
            return (int)dbKeys["PersonId"];
        }

        public override Person AfterAutoValueRetrieved(Person src, int identity)
        {
            src.PersonId = identity;

            return src;
        }
    }
}
