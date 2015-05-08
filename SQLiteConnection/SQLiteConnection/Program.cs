using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SQLite;
using System.IO;

namespace SQLite
{
    class SQLiteConnecter
    {

        static int db_size;

        static void Main(string[] args)
        {


            SQLiteConnection m_dbConnection;

            SQLiteConnection.CreateFile("autompg.sqlite");
            m_dbConnection = new SQLiteConnection("Data Source=autompg.sqlite;Version=3;");
            m_dbConnection.Open();

            string s;
            StreamReader str = new StreamReader("autompg.sql");
            s = str.ReadToEnd();
            SQLiteCommand command = new SQLiteCommand(s, m_dbConnection);
            try
            {
                command.ExecuteNonQuery();

                command = new SQLiteCommand("select * from autompg order by cylinders desc", m_dbConnection);

                db_size = 0;
                SQLiteDataReader reader = command.ExecuteReader();
                while (reader.Read())
                {
                    db_size++;
                    //Console.WriteLine("Brand: " + reader["brand"] + "\tCylinder: " + reader["cylinders"]);
                }


                Console.WriteLine("IDF(renault): " + IDF("brand","renault",m_dbConnection));
                Console.WriteLine("IDF(buick): " + IDF("brand", "buick", m_dbConnection));
                Console.WriteLine("IDF(chevrolet): " + IDF("brand", "chevrolet", m_dbConnection));
                Console.WriteLine("IDF(amc): " + IDF("brand", "amc", m_dbConnection));
                Console.WriteLine("IDF(plymouth): " + IDF("brand", "plymouth", m_dbConnection));
                Console.WriteLine("IDF(toyota): " + IDF("brand", "toyota", m_dbConnection));
                Console.WriteLine("IDF(ford): " + IDF("brand", "ford", m_dbConnection));
                Console.WriteLine("IDF(volkswagen): " + IDF("brand", "volkswagen", m_dbConnection));

                

            }

            finally
            {
                m_dbConnection.Close();
            }



            /*
            SQLiteConnection m_dbConnection;
            SQLiteConnection.CreateFile("MyDatabase.sqlite");
            m_dbConnection = new SQLiteConnection("Data Source=MyDatabase.sqlite;Version=3;");
            m_dbConnection.Open();

            string sql = "create table highscores (name varchar(20), score int)";
            SQLiteCommand command = new SQLiteCommand(sql, m_dbConnection);

            command.ExecuteNonQuery();

            sql = "insert into highscores (name, score) values ('Me', 3000)";
            SQLiteCommand command2 = new SQLiteCommand(sql, m_dbConnection);
            command2.ExecuteNonQuery();
            sql = "insert into highscores (name, score) values ('Myself', 6000)";
            command2 = new SQLiteCommand(sql, m_dbConnection);
            command2.ExecuteNonQuery();
            sql = "insert into highscores (name, score) values ('And I', 9001)";
            command2 = new SQLiteCommand(sql, m_dbConnection);
            command2.ExecuteNonQuery();

            string sql2 = "select * from highscores order by score desc";
            SQLiteCommand command3 = new SQLiteCommand(sql2, m_dbConnection);

            SQLiteDataReader reader = command3.ExecuteReader();

            string sql3 = "select * from highscores order by score desc";
            SQLiteCommand command4 = new SQLiteCommand(sql3, m_dbConnection);
            SQLiteDataReader reader2 = command4.ExecuteReader();
            while (reader2.Read())
                Console.WriteLine("Name: " + reader2["name"] + "\tScore: " + reader2["score"]);
            */
            Console.Read();
        }


        private static double IDF(string category, string value, SQLiteConnection db)
        {
            string q = "select " + category + " from autompg where " + category + "=\'" + value + "\'";
            SQLiteCommand command = new SQLiteCommand(q, db);
            SQLiteDataReader reader = command.ExecuteReader();
            int freq = 0;

            // tel hoeveel matches
            while (reader.Read()) { freq++;};
            
            return Math.Log10(db_size / freq);
        }

        private static double IDF(string category, double value, SQLiteConnection db)
        {

            return 0;
        }


    }
}
