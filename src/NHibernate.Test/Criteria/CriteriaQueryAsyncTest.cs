using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NHibernate.Criterion;
using NHibernate.SqlCommand;
using NHibernate.Transform;
using NHibernate.Type;
using NUnit.Framework;

namespace NHibernate.Test.Criteria
{
	[TestFixture]
	public class CriteriaQueryAsyncTest : TestCase
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

		[Test]
		[ExpectedException(typeof(AggregateException))]
		public void ListAsync_ShouldReturnCanceledTaskWhenPassedCanceledToken()
		{
			// Arrange
			var cancellationTokenSource = new CancellationTokenSource();
			cancellationTokenSource.Cancel();
			Task<IList<Student>> result;
			using (ISession session = OpenSession())
			{
				// Act
				result = session.CreateCriteria<Student>()
					.ListAsync<Student>(cancellationTokenSource.Token);
			}

			// Assert
			Assert.That(result.IsCanceled);
			result.Wait();
		}

		[Test]
		public void ListAsync_ShouldThrowExceptionWhenCancellationTokenIsCanceled()
		{
			// Arrange
			var cancellationTokenSource = new CancellationTokenSource();
			Task task;
			using (ISession session = OpenSession())
			{
				// Act
				task = session.CreateCriteria<Student>()
					.ListAsync<Student>(cancellationTokenSource.Token);

				cancellationTokenSource.Cancel();
			}

			try
			{
				task.Wait();
			}
			catch (AggregateException aggregateException)
			{
				// Assert
				Assert.That(aggregateException.InnerExceptions.Count, Is.EqualTo(1));
				Assert.That(aggregateException.InnerExceptions[0], Is.TypeOf(typeof(TaskCanceledException)));
			}
		}

		[Test]
		public void ListAsync_fetchesAllStudents()
		{
			// Arrange
			using (ISession session = OpenSession())
			using (ITransaction t = session.BeginTransaction())
			{
				session.Save(new Student { Name = "Mengano", StudentNumber = 232 });
				session.Save(new Student { Name = "Ayende", StudentNumber = 999 });
				session.Save(new Student { Name = "Fabio", StudentNumber = 123 });
				session.Save(new Student { Name = "Merlo", StudentNumber = 456 });
				session.Save(new Student { Name = "Fulano", StudentNumber = 0 });

				t.Commit();
			}

			using (ISession session = OpenSession())
			using (ITransaction t = session.BeginTransaction())
			{
				// Act
				session.CreateCriteria<Student>()
					.ListAsync<Student>()
					.ContinueWith(task =>
					{
						var result = task.Result;

						// Assert
						Assert.AreEqual(result.Count, 5);
					}).Wait();

				t.Commit();
			}

			using (ISession session = OpenSession())
			using (ITransaction t = session.BeginTransaction())
			{
				session.CreateQuery("delete from Student").ExecuteUpdate();
				t.Commit();
			}
		}

		[Test]
		public void ListAsync_fetchesAllStudentsMultipleTimes()
		{
			// Arrange
			const int numberOfTasks = 4;
			var tasks = new Task<IList<Student>>[numberOfTasks];

			using (ISession session = OpenSession())
			using (ITransaction t = session.BeginTransaction())
			{
				session.Save(new Student { Name = "Mengano", StudentNumber = 232 });
				session.Save(new Student { Name = "Ayende", StudentNumber = 999 });
				session.Save(new Student { Name = "Fabio", StudentNumber = 123 });
				session.Save(new Student { Name = "Merlo", StudentNumber = 456 });
				session.Save(new Student { Name = "Fulano", StudentNumber = 0 });

				t.Commit();
			}

			using (ISession session = OpenSession())
			using (ITransaction t = session.BeginTransaction())
			{
				// Act
				for (int i = 0; i < numberOfTasks; i++)
				{
					var task = session.CreateCriteria<Student>()
					.ListAsync<Student>();
					tasks[i] = task;
				}

				Task.Factory.ContinueWhenAll(tasks, finishedTasks =>
				{
					// Assert: No Exceptions
					// Assert: all tasks return all students
					foreach (var task in finishedTasks)
					{
						Assert.AreEqual(task.Result.Count, 5);
					}
				}).Wait();

				t.Commit();
			}

			using (ISession session = OpenSession())
			using (ITransaction t = session.BeginTransaction())
			{
				session.CreateQuery("delete from Student").ExecuteUpdate();
				t.Commit();
			}
		}

		[Test]
		public void SubqueryPaginationOnlyWithFirstAsync()
		{
			// Arrange
			using (ISession session = OpenSession())
			using (ITransaction t = session.BeginTransaction())
			{
				session.Save(new Student { Name = "Mengano", StudentNumber = 232 });
				session.Save(new Student { Name = "Ayende", StudentNumber = 999 });
				session.Save(new Student { Name = "Fabio", StudentNumber = 123 });
				session.Save(new Student { Name = "Merlo", StudentNumber = 456 });
				session.Save(new Student { Name = "Fulano", StudentNumber = 0 });

				t.Commit();
			}

			using (ISession session = OpenSession())
			using (ITransaction t = session.BeginTransaction())
			{
				DetachedCriteria dc = DetachedCriteria.For(typeof(Student))
					.Add(Property.ForName("StudentNumber").Gt(0L))
					.SetFirstResult(1)
					.AddOrder(Order.Asc("StudentNumber"))
					.SetProjection(Property.ForName("Name"));

				// Act
				session.CreateCriteria(typeof(Student))
					.Add(Subqueries.PropertyIn("Name", dc))
					.ListAsync<Student>()
					.ContinueWith(task =>
					{
						var result = task.Result;

						// Assert
						Assert.That(result.Count, Is.EqualTo(3));
					}).Wait();

				t.Commit();
			}

			using (ISession session = OpenSession())
			using (ITransaction t = session.BeginTransaction())
			{
				session.CreateQuery("delete from Student").ExecuteUpdate();
				t.Commit();
			}
		}

		[Test]
		public void SubqueryPaginationAsync()
		{
			// Arrange
			using (ISession session = OpenSession())
			using (ITransaction t = session.BeginTransaction())
			{
				session.Save(new Student { Name = "Mengano", StudentNumber = 232 });
				session.Save(new Student { Name = "Ayende", StudentNumber = 999 });
				session.Save(new Student { Name = "Fabio", StudentNumber = 123 });
				session.Save(new Student { Name = "Merlo", StudentNumber = 456 });
				session.Save(new Student { Name = "Fulano", StudentNumber = 0 });

				t.Commit();
			}

			using (ISession session = OpenSession())
			using (ITransaction t = session.BeginTransaction())
			{
				DetachedCriteria dc = DetachedCriteria.For(typeof(Student))
					.Add(Property.ForName("StudentNumber").Gt(200L))
					.SetMaxResults(2)
					.SetFirstResult(1)
					.AddOrder(Order.Asc("StudentNumber"))
					.SetProjection(Property.ForName("Name"));

				// Act
				session.CreateCriteria(typeof(Student))
					.Add(Subqueries.PropertyIn("Name", dc))
					.AddOrder(Order.Asc("StudentNumber"))
					.ListAsync<Student>()
					.ContinueWith(task =>
					{
						var result = task.Result;

						// Assert
						Assert.That(result.Count, Is.EqualTo(2));
						Assert.That(result[0].StudentNumber, Is.EqualTo(456));
						Assert.That(result[1].StudentNumber, Is.EqualTo(999));
					}).Wait();

				t.Commit();
			}

			using (ISession session = OpenSession())
			using (ITransaction t = session.BeginTransaction())
			{
				session.CreateQuery("delete from Student").ExecuteUpdate();
				t.Commit();
			}
		}

		[Test]
		public void SimplePaginationAsync()
		{
			// Arrange
			using (ISession session = OpenSession())
			using (ITransaction t = session.BeginTransaction())
			{
				session.Save(new Student { Name = "Mengano", StudentNumber = 232 });
				session.Save(new Student { Name = "Ayende", StudentNumber = 999 });
				session.Save(new Student { Name = "Fabio", StudentNumber = 123 });
				session.Save(new Student { Name = "Merlo", StudentNumber = 456 });
				session.Save(new Student { Name = "Fulano", StudentNumber = 0 });

				t.Commit();
			}

			using (ISession session = OpenSession())
			using (ITransaction t = session.BeginTransaction())
			{
				// Act
				session.CreateCriteria<Student>()
					.Add(Restrictions.Gt("StudentNumber", 0L))
					.AddOrder(Order.Asc("StudentNumber"))
					.SetFirstResult(1).SetMaxResults(2)
					.ListAsync<Student>()
					.ContinueWith(task =>
					{
						var result = task.Result;

						// Assert
						Assert.That(result.Count, Is.EqualTo(2));
						Assert.That(result[0].StudentNumber, Is.EqualTo(232));
						Assert.That(result[1].StudentNumber, Is.EqualTo(456));
					}).Wait();

				t.Commit();
			}

			using (ISession session = OpenSession())
			using (ITransaction t = session.BeginTransaction())
			{
				session.CreateQuery("delete from Student").ExecuteUpdate();
				t.Commit();
			}
		}

		[Test]
		public void SimplePaginationOnlyWithFirstAsync()
		{
			// Arrange
			using (ISession session = OpenSession())
			using (ITransaction t = session.BeginTransaction())
			{
				session.Save(new Student { Name = "Mengano", StudentNumber = 232 });
				session.Save(new Student { Name = "Ayende", StudentNumber = 999 });
				session.Save(new Student { Name = "Fabio", StudentNumber = 123 });
				session.Save(new Student { Name = "Merlo", StudentNumber = 456 });
				session.Save(new Student { Name = "Fulano", StudentNumber = 0 });

				t.Commit();
			}

			using (ISession session = OpenSession())
			using (ITransaction t = session.BeginTransaction())
			{
				// Act
				session.CreateCriteria<Student>()
					.Add(Restrictions.Gt("StudentNumber", 0L))
					.AddOrder(Order.Asc("StudentNumber"))
					.SetFirstResult(1)
					.ListAsync<Student>()
					.ContinueWith(task =>
					{
						var result = task.Result;

						// Assert
						Assert.That(result.Count, Is.EqualTo(3));
						Assert.That(result[0].StudentNumber, Is.EqualTo(232));
						Assert.That(result[1].StudentNumber, Is.EqualTo(456));
						Assert.That(result[2].StudentNumber, Is.EqualTo(999));
					}).Wait();

				t.Commit();
			}

			using (ISession session = OpenSession())
			using (ITransaction t = session.BeginTransaction())
			{
				session.CreateQuery("delete from Student").ExecuteUpdate();
				t.Commit();
			}
		}

		[Test]
		public void ProjectionsUsingPropertyAsync()
		{
			ISession s = OpenSession();
			ITransaction t = s.BeginTransaction();

			Course course = new Course();
			course.CourseCode = "HIB";
			course.Description = "Hibernate Training";
			course.CourseMeetings.Add(new CourseMeeting(course, "Monday", 1, "1313 Mockingbird Lane"));
			s.Save(course);

			Student gavin = new Student();
			gavin.Name = "Gavin King";
			gavin.StudentNumber = 667;
			CityState odessaWa = new CityState("Odessa", "WA");
			gavin.CityState = odessaWa;
			gavin.PreferredCourse = course;
			s.Save(gavin);

			Student xam = new Student();
			xam.Name = "Max Rydahl Andersen";
			xam.StudentNumber = 101;
			s.Save(xam);

			Enrolment enrolment = new Enrolment();
			enrolment.Course = course;
			enrolment.CourseCode = course.CourseCode;
			enrolment.Semester = 1;
			enrolment.Year = 1999;
			enrolment.Student = xam;
			enrolment.StudentNumber = xam.StudentNumber;
			xam.Enrolments.Add(enrolment);
			s.Save(enrolment);

			enrolment = new Enrolment();
			enrolment.Course = course;
			enrolment.CourseCode = course.CourseCode;
			enrolment.Semester = 3;
			enrolment.Year = 1998;
			enrolment.Student = gavin;
			enrolment.StudentNumber = gavin.StudentNumber;
			gavin.Enrolments.Add(enrolment);
			s.Save(enrolment);
			s.Flush();

			// Subtest #1
			IList resultList = null;
			s.CreateCriteria<Enrolment>()
				.SetProjection(Projections.ProjectionList()
					.Add(Property.ForName("Student"), "student")
					.Add(Property.ForName("Course"), "course")
					.Add(Property.ForName("Semester"), "semester")
					.Add(Property.ForName("Year"), "year")
				).ListAsync().ContinueWith(task =>
				{
					resultList = task.Result;

					Assert.That(resultList.Count, Is.EqualTo(2));

					foreach (object[] objects in resultList)
					{
						Assert.That(objects.Length, Is.EqualTo(4));
						Assert.That(objects[0], Is.InstanceOf<Student>());
						Assert.That(objects[1], Is.InstanceOf<Course>());
						Assert.That(objects[2], Is.InstanceOf<short>());
						Assert.That(objects[3], Is.InstanceOf<short>());
					}
				}).Wait();

			// Subtest #2
			s.CreateCriteria<Student>()
				.SetProjection(Projections.ProjectionList()
					.Add(Projections.Id().As("StudentNumber"))
					.Add(Property.ForName("Name"), "name")
					.Add(Property.ForName("CityState"), "cityState")
					.Add(Property.ForName("PreferredCourse"), "preferredCourse")
				).ListAsync().ContinueWith(task =>
				{
					resultList = task.Result;

					Assert.That(resultList.Count, Is.EqualTo(2));

					foreach (object[] objects in resultList)
					{
						Assert.That(objects.Length, Is.EqualTo(4));
						Assert.That(objects[0], Is.InstanceOf<long>());
						Assert.That(objects[1], Is.InstanceOf<string>());

						if ("Gavin King".Equals(objects[1]))
						{
							Assert.That(objects[2], Is.InstanceOf<CityState>());
							Assert.That(objects[3], Is.InstanceOf<Course>());
						}
						else
						{
							Assert.That(objects[2], Is.Null);
							Assert.That(objects[3], Is.Null);
						}
					}
				}).Wait();

			// Subtest #3
			s.CreateCriteria<Student>()
				.Add(Restrictions.Eq("Name", "Gavin King"))
				.SetProjection(Projections.ProjectionList()
					.Add(Projections.Id().As("StudentNumber"))
					.Add(Property.ForName("Name"), "name")
					.Add(Property.ForName("CityState"), "cityState")
					.Add(Property.ForName("PreferredCourse"), "preferredCourse")
				).ListAsync().ContinueWith(task =>
				{
					resultList = task.Result;

					Assert.That(resultList.Count, Is.EqualTo(1));
				}).Wait();

			// Subtest #4
			s.CreateCriteria<Student>()
				.Add(Restrictions.IdEq(667L))
				.SetProjection(Projections.ProjectionList()
					.Add(Projections.Id().As("StudentNumber"))
					.Add(Property.ForName("Name"), "name")
					.Add(Property.ForName("CityState"), "cityState")
					.Add(Property.ForName("PreferredCourse"), "preferredCourse")
				).UniqueResultAsync().ContinueWith(task =>
				{
					object[] aResult = (object[])task.Result;

					Assert.That(aResult, Is.Not.Null);
					Assert.That(aResult.Length, Is.EqualTo(4));
					Assert.That(aResult[0], Is.InstanceOf<long>());
					Assert.That(aResult[1], Is.InstanceOf<string>());
					Assert.That(aResult[2], Is.InstanceOf<CityState>());
					Assert.That(aResult[3], Is.InstanceOf<Course>());
				}).Wait();

			// Subtest #5
			s.CreateCriteria(typeof(Enrolment))
								.SetProjection(Property.ForName("StudentNumber").Count().SetDistinct())
								.UniqueResultAsync()
								.ContinueWith(task =>
									Assert.AreEqual(2, task.Result)).Wait();

			// Subtest #6
			s.CreateCriteria(typeof(Enrolment))
				.SetProjection(Projections.ProjectionList()
								.Add(Property.ForName("StudentNumber").Count())
								.Add(Property.ForName("StudentNumber").Max())
								.Add(Property.ForName("StudentNumber").Min())
								.Add(Property.ForName("StudentNumber").Avg())
				)
				.UniqueResultAsync()
				.ContinueWith(task =>
				{
					object[] result = (object[])task.Result;

					Assert.AreEqual(2, result[0]);
					Assert.AreEqual(667L, result[1]);
					Assert.AreEqual(101L, result[2]);
					Assert.AreEqual(384.0D, (double)result[3], 0.01D);
				}).Wait();

			// Subtest #7
			s.CreateCriteria(typeof(Enrolment))
				.Add(Property.ForName("StudentNumber").Gt(665L))
				.Add(Property.ForName("StudentNumber").Lt(668L))
				.Add(Property.ForName("CourseCode").Like("HIB", MatchMode.Start))
				.Add(Property.ForName("Year").Eq((short)1999))
				.AddOrder(Property.ForName("StudentNumber").Asc())
				.UniqueResultAsync().Wait();

			// Subtest #8
			IList resultWithMaps = null;
			IDictionary m1 = null;
			s.CreateCriteria(typeof(Enrolment))
				.SetProjection(Projections.ProjectionList()
								.Add(Property.ForName("StudentNumber").As("stNumber"))
								.Add(Property.ForName("CourseCode").As("cCode"))
				)
				.Add(Property.ForName("StudentNumber").Gt(665L))
				.Add(Property.ForName("StudentNumber").Lt(668L))
				.AddOrder(Property.ForName("StudentNumber").Asc())
				.SetResultTransformer(CriteriaSpecification.AliasToEntityMap)
				.ListAsync().ContinueWith(task =>
				{
					resultWithMaps = task.Result;

					Assert.AreEqual(1, resultWithMaps.Count);

					m1 = (IDictionary)resultWithMaps[0];
					Assert.AreEqual(667L, m1["stNumber"]);
					Assert.AreEqual(course.CourseCode, m1["cCode"]);
				}).Wait();

			// Subtest #9
			s.CreateCriteria(typeof(Enrolment))
				.SetProjection(Property.ForName("StudentNumber").As("stNumber"))
				.AddOrder(Order.Desc("stNumber"))
				.SetResultTransformer(CriteriaSpecification.AliasToEntityMap)
				.ListAsync()
				.ContinueWith(task =>
				{
					resultWithMaps = task.Result;

					Assert.AreEqual(2, resultWithMaps.Count);
					IDictionary m0 = (IDictionary)resultWithMaps[0];
					m1 = (IDictionary)resultWithMaps[1];
					Assert.AreEqual(101L, m1["stNumber"]);
					Assert.AreEqual(667L, m0["stNumber"]);
				}).Wait();

			// Subtest #10
			IList resultWithAliasedBean = null;
			s.CreateCriteria(typeof(Enrolment))
				.CreateAlias("Student", "st")
				.CreateAlias("Course", "co")
				.SetProjection(Projections.ProjectionList()
					.Add(Property.ForName("st.Name").As("studentName"))
					.Add(Property.ForName("co.Description").As("courseDescription"))
				)
				.AddOrder(Order.Desc("studentName"))
				.SetResultTransformer(Transformers.AliasToBean(typeof(StudentDTO)))
				.ListAsync()
				.ContinueWith(task =>
				{
					resultWithAliasedBean = task.Result;

					Assert.AreEqual(2, resultWithAliasedBean.Count);

					// Subtest #11
					StudentDTO dto = (StudentDTO)resultWithAliasedBean[0];
					Assert.IsNotNull(dto.Description);
					Assert.IsNotNull(dto.Name);
				}).Wait();

			// Subtest #12
			s.CreateCriteria<CourseMeeting>()
				.SetProjection(Projections.ProjectionList()
					.Add(Property.ForName("Id").As("id"))
					.Add(Property.ForName("Course").As("course"))
				)
				.AddOrder(Order.Desc("id"))
				.SetResultTransformer(Transformers.AliasToBean<CourseMeeting>())
				.UniqueResultAsync<CourseMeeting>()
				.ContinueWith(task =>
				{
					CourseMeeting courseMeetingDto = task.Result;

					Assert.That(courseMeetingDto.Id, Is.Not.Null);
					Assert.That(courseMeetingDto.Id.CourseCode, Is.EqualTo(course.CourseCode));
					Assert.That(courseMeetingDto.Id.Day, Is.EqualTo("Monday"));
					Assert.That(courseMeetingDto.Id.Location, Is.EqualTo("1313 Mockingbird Lane"));
					Assert.That(courseMeetingDto.Id.Period, Is.EqualTo(1));
					Assert.That(courseMeetingDto.Course.Description, Is.EqualTo(course.Description));
				}).Wait();

			// Subtest #13
			s.CreateCriteria(typeof(Student))
				.Add(Expression.Like("Name", "Gavin", MatchMode.Start))
				.AddOrder(Order.Asc("Name"))
				.CreateCriteria("Enrolments", "e")
				.AddOrder(Order.Desc("Year"))
				.AddOrder(Order.Desc("Semester"))
				.CreateCriteria("Course", "c")
				.AddOrder(Order.Asc("Description"))
				.SetProjection(Projections.ProjectionList()
					.Add(Property.ForName("this.Name"))
					.Add(Property.ForName("e.Year"))
					.Add(Property.ForName("e.Semester"))
					.Add(Property.ForName("c.CourseCode"))
					.Add(Property.ForName("c.Description"))
				)
				.UniqueResultAsync().Wait();

			// Subtest #14
			ProjectionList p1 = Projections.ProjectionList()
				.Add(Property.ForName("StudentNumber").Count())
				.Add(Property.ForName("StudentNumber").Max())
				.Add(Projections.RowCount());

			ProjectionList p2 = Projections.ProjectionList()
				.Add(Property.ForName("StudentNumber").Min())
				.Add(Property.ForName("StudentNumber").Avg())
				.Add(Projections.SqlProjection(
					"1 as constOne, count(*) as countStar",
					new String[] { "constOne", "countStar" },
					new IType[] { NHibernateUtil.Int32, NHibernateUtil.Int32 }
					));

			s.CreateCriteria(typeof(Enrolment))
				.SetProjection(Projections.ProjectionList().Add(p1).Add(p2))
				.UniqueResultAsync()
				.ContinueWith(task =>
				{
					object[] array = (object[])task.Result;
					Assert.AreEqual(7, array.Length);
				}).Wait();

			// Subtest #15
			IList list = null;
			s.CreateCriteria(typeof(Enrolment))
				.CreateAlias("Student", "st")
				.CreateAlias("Course", "co")
				.SetProjection(Projections.ProjectionList()
					.Add(Property.ForName("co.CourseCode").Group())
					.Add(Property.ForName("st.StudentNumber").Count().SetDistinct())
					.Add(Property.ForName("Year").Group())
				)
				.ListAsync()
				.ContinueWith(task =>
				{
					list = task.Result;

					Assert.AreEqual(2, list.Count);
				}).Wait();

			// Subtest #16
			s.CreateCriteria<Enrolment>()
						.CreateAlias("Student", "st")
						.CreateAlias("Course", "co")
						.SetProjection(Projections.ProjectionList()
							.Add(Property.ForName("co.CourseCode").Group().As("courseCode"))
							.Add(Property.ForName("st.StudentNumber").Count().SetDistinct().As("studentNumber"))
							.Add(Property.ForName("Year").Group())
				)
				.AddOrder(Order.Asc("courseCode"))
				.AddOrder(Order.Asc("studentNumber"))
				.ListAsync()
				.ContinueWith(task =>
				{
					list = task.Result;

					Assert.That(list.Count, Is.EqualTo(2));
				}).Wait();

			// Subtest #17
			s.CreateCriteria<Enrolment>()
				.CreateAlias("Student", "st")
				.CreateAlias("Course", "co")
				.SetProjection(Projections.ProjectionList()
					.Add(Property.ForName("co.CourseCode").Group().As("cCode"))
					.Add(Property.ForName("st.StudentNumber").Count().SetDistinct().As("stNumber"))
					.Add(Property.ForName("Year").Group())
				)
				.AddOrder(Order.Asc("cCode"))
				.AddOrder(Order.Asc("stNumber"))
				.ListAsync()
				.ContinueWith(task =>
				{
					list = task.Result;

					Assert.That(list.Count, Is.EqualTo(2));
				}).Wait();

			s.Delete(gavin);
			s.Delete(xam);
			s.Delete(course);

			t.Commit();
			s.Close();
		}

		[Test]
		public void PropertyWithFormulaAndPagingAsyncTest()
		{
			ISession s = OpenSession();
			ITransaction t = s.BeginTransaction();

			ICriteria crit = s.CreateCriteria(typeof(Animal))
				.SetFirstResult(0)
				.SetMaxResults(1)
				.AddOrder(new Order("bodyWeight", true));

			crit.ListAsync<Animal>()
				.ContinueWith(_ =>
				{
					t.Rollback();
					s.Close();
				}).Wait();
		}
	}
}