using System.Collections;
using System.Collections.Generic;
using NHibernate.Criterion;
using NHibernate.Dialect;
using NUnit.Framework;

namespace NHibernate.Test.Criteria
{
	[TestFixture]
    public class ProjectionsAsyncTest : TestCase
	{
		protected override string MappingsAssembly
		{
			get { return "NHibernate.Test"; }
		}

		protected override IList Mappings
		{
			get
			{
				return new string[]
					{
						"Criteria.Enrolment.hbm.xml",
						"Criteria.Animal.hbm.xml",
						"Criteria.MaterialResource.hbm.xml"
					};
			}
		}

		protected override void OnSetUp()
		{
			using (ISession session = OpenSession())
			{
				ITransaction t = session.BeginTransaction();

				Student gavin = new Student();
				gavin.Name = "ayende";
				gavin.StudentNumber = 27;
				session.Save(gavin);

				t.Commit();
			}
		}

		protected override void OnTearDown()
		{
			using (ISession session = sessions.OpenSession())
			{
				session.Delete("from System.Object");
				session.Flush();
			}
		}

		[Test]
		public void UseInWithProjectionAsync()
		{
			using (ISession session = sessions.OpenSession())
			{
				IList<Student> list = session.CreateCriteria(typeof(Student))
					.Add(Expression.In(Projections.Id(), new object[] { 27 }))
					.ListAsync<Student>().Result;
				Assert.AreEqual(27L, list[0].StudentNumber);
			}
		}

		[Test]
        public void UseLikeWithProjectionAsync()
		{
			using (ISession session = sessions.OpenSession())
			{
				IList<Student> list = session.CreateCriteria(typeof(Student))
					.Add(Expression.Like(Projections.Property("Name"), "aye", MatchMode.Start))
                    .ListAsync<Student>().Result;
				Assert.AreEqual(27L, list[0].StudentNumber);
			}
		}

		[Test]
        public void UseInsensitiveLikeWithProjectionAsync()
		{
			using (ISession session = sessions.OpenSession())
			{
				IList<Student> list = session.CreateCriteria(typeof(Student))
					.Add(Expression.InsensitiveLike(Projections.Property("Name"), "AYE", MatchMode.Start))
                    .ListAsync<Student>().Result;
				Assert.AreEqual(27L, list[0].StudentNumber);
			}
		}

		[Test]
        public void UseIdEqWithProjectionAsync()
		{
			using (ISession session = sessions.OpenSession())
			{
				IList<Student> list = session.CreateCriteria(typeof(Student))
					.Add(Expression.IdEq(Projections.Id()))
                    .ListAsync<Student>().Result;
				Assert.AreEqual(27L, list[0].StudentNumber);
			}
		}

		[Test]
        public void UseEqWithProjectionAsync()
		{
			using (ISession session = sessions.OpenSession())
			{
				IList<Student> list = session.CreateCriteria(typeof(Student))
					.Add(Expression.Eq(Projections.Id(), 27L))
                    .ListAsync<Student>().Result;
				Assert.AreEqual(27L, list[0].StudentNumber);
			}
		}


		[Test]
        public void UseGtWithProjectionAsync()
		{
			using (ISession session = sessions.OpenSession())
			{
				IList<Student> list = session.CreateCriteria(typeof(Student))
					.Add(Expression.Gt(Projections.Id(), 2L))
                    .ListAsync<Student>().Result;
				Assert.AreEqual(27L, list[0].StudentNumber);
			}
		}

		[Test]
        public void UseLtWithProjectionAsync()
		{
			using (ISession session = sessions.OpenSession())
			{
				IList<Student> list = session.CreateCriteria(typeof(Student))
					.Add(Expression.Lt(Projections.Id(), 200L))
                    .ListAsync<Student>().Result;
				Assert.AreEqual(27L, list[0].StudentNumber);
			}
		}

		[Test]
        public void UseLeWithProjectionAsync()
		{
			using (ISession session = sessions.OpenSession())
			{
				IList<Student> list = session.CreateCriteria(typeof(Student))
					.Add(Expression.Le(Projections.Id(), 27L))
                    .ListAsync<Student>().Result;
				Assert.AreEqual(27L, list[0].StudentNumber);
			}
		}

		[Test]
        public void UseGeWithProjectionAsync()
		{
			using (ISession session = sessions.OpenSession())
			{
				IList<Student> list = session.CreateCriteria(typeof(Student))
					.Add(Expression.Ge(Projections.Id(), 27L))
                    .ListAsync<Student>().Result;
				Assert.AreEqual(27L, list[0].StudentNumber);
			}
		}

		[Test]
        public void UseBetweenWithProjectionAsync()
		{
			using (ISession session = sessions.OpenSession())
			{
				IList<Student> list = session.CreateCriteria(typeof(Student))
					.Add(Expression.Between(Projections.Id(), 10L, 28L))
                    .ListAsync<Student>().Result;
				Assert.AreEqual(27L, list[0].StudentNumber);
			}
		}

		[Test]
        public void UseIsNullWithProjectionAsync()
		{
			using (ISession session = sessions.OpenSession())
			{
				IList<Student> list = session.CreateCriteria(typeof(Student))
					.Add(Expression.IsNull(Projections.Id()))
                    .ListAsync<Student>().Result;
				Assert.AreEqual(0, list.Count);
			}
		}

		[Test]
        public void UseIsNotNullWithProjectionAsync()
		{
			using (ISession session = sessions.OpenSession())
			{
				IList<Student> list = session.CreateCriteria(typeof(Student))
					.Add(Expression.IsNotNull(Projections.Id()))
                    .ListAsync<Student>().Result;
				Assert.AreEqual(1, list.Count);
			}
		}

		[Test]
        public void UseEqPropertyWithProjectionAsync()
		{
			using (ISession session = sessions.OpenSession())
			{
				IList<Student> list = session.CreateCriteria(typeof(Student))
					.Add(Expression.EqProperty(Projections.Id(), Projections.Id()))
                    .ListAsync<Student>().Result;
				Assert.AreEqual(1, list.Count);
			}
		}

		[Test]
        public void UseGePropertyWithProjectionAsync()
		{
			using (ISession session = sessions.OpenSession())
			{
				IList<Student> list = session.CreateCriteria(typeof(Student))
					.Add(Expression.GeProperty(Projections.Id(), Projections.Id()))
                    .ListAsync<Student>().Result;
				Assert.AreEqual(1, list.Count);
			}
		}

		[Test]
        public void UseGtPropertyWithProjectionAsync()
		{
			using (ISession session = sessions.OpenSession())
			{
				IList<Student> list = session.CreateCriteria(typeof(Student))
					.Add(Expression.GtProperty(Projections.Id(), Projections.Id()))
                    .ListAsync<Student>().Result;
				Assert.AreEqual(0, list.Count);
			}
		}

		[Test]
        public void UseLtPropertyWithProjectionAsync()
		{
			using (ISession session = sessions.OpenSession())
			{
				IList<Student> list = session.CreateCriteria(typeof(Student))
					.Add(Expression.LtProperty(Projections.Id(), Projections.Id()))
                    .ListAsync<Student>().Result;
				Assert.AreEqual(0, list.Count);
			}
		}

		[Test]
        public void UseLePropertyWithProjectionAsync()
		{
			using (ISession session = sessions.OpenSession())
			{
				IList<Student> list = session.CreateCriteria(typeof(Student))
					.Add(Expression.LeProperty(Projections.Id(), Projections.Id()))
                    .ListAsync<Student>().Result;
				Assert.AreEqual(1, list.Count);
			}
		}

		[Test]
        public void UseNotEqPropertyWithProjectionAsync()
		{
			using (ISession session = sessions.OpenSession())
			{
				IList<Student> list = session.CreateCriteria(typeof(Student))
					.Add(Expression.NotEqProperty("id", Projections.Id()))
                    .ListAsync<Student>().Result;
				Assert.AreEqual(0, list.Count);
			}
		}


	}
}
