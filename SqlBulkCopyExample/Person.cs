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
        public Person()
        {
            Kids = new List<Kid> { };
        }

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

    public class PersonToKidMapping : Kid
    {
        public int PersonId { get; set; }

        public PersonToKidMapping(Person parent, Kid kid)
        {
            base.Age = kid.Age;
            base.KidId = kid.KidId;
            
            this.PersonId = parent.PersonId;
        }
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
            Identity(x => x.PersonId, dbType: "INT");
            Column(x => x.Name);
            Column(x => x.DateOfBirth);
            Column("AreaCode", x => x.PhoneNumber.AreaCode);
            Column("Number", x => x.PhoneNumber.Number);
            Column("FullPhoneNumber", x => x.PhoneNumber.AreaCode  + "-" + x.PhoneNumber.Number);
            Column("HasKids", x => x.Kids.Any());
            Column("SumOfKidsAge", x => x.Kids.Sum(y => y.Age));
        }

        public override Person AfterInsert(Person item, IDictionary<string, object> identities)
        {
            item.PersonId = (int)identities["PersonId"];

            return item;
        }
    }

    public class KidInserter : BaseInserter<Tuple<int, Kid>>
    {
        public KidInserter()
            : base("Kid")
        {
            Identity("KidId", dbType: "INT");
            Column("PersonId", x => x.Item1);
            Column("Age", x => x.Item2.Age);
        }

        public override Tuple<int, Kid> AfterInsert(Tuple<int, Kid> item, IDictionary<string, object> identities)
        {
            item.Item2.KidId = (int)identities["KidId"];

            return item;
        }
    }

    public class KidInserter2 : BaseInserter<PersonToKidMapping>
    {
        public KidInserter2()
            : base("Kid")
        {
            Identity(x => x.KidId, "INT");
            Column(x => x.PersonId);
            Column(x => x.Age);
        }

        public override PersonToKidMapping AfterInsert(PersonToKidMapping item, IDictionary<string, object> identities)
        {
            item.KidId = (int)identities["KidId"];

            return item;
        }
    }
}
