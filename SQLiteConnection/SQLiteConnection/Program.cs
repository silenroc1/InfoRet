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
        static ISet<string> brand_values;
        static ISet<string> model_values;
        static ISet<string> type_values;

        static void Main(string[] args)
        {

            // inladen van autompg
            SQLiteConnection m_dbConnection;

            SQLiteConnection.CreateFile("autompg.sqlite");
            m_dbConnection = new SQLiteConnection("Data Source=autompg.sqlite;Version=3;");
            m_dbConnection.Open();

            string s;
            StreamReader str = new StreamReader("autompg.sql");
            s = str.ReadToEnd();
            SQLiteCommand command = new SQLiteCommand(s, m_dbConnection);
            command.ExecuteNonQuery();


            // meta-db voor idf en tf
            SQLiteConnection meta_db;
            SQLiteConnection.CreateFile("meta_db.sqlite");
            meta_db = new SQLiteConnection("Data Source=meta_db.sqlite;Version=3;");
            meta_db.Open();

            s = "create table idf_cat (category varchar(20), value varchar(20), score double)";
            SQLiteCommand meta_dbCommand = new SQLiteCommand(s, meta_db);
            meta_dbCommand.ExecuteNonQuery();
            

            s = "create table idf_num (category varchar(20), value double, score double)";
            meta_dbCommand = new SQLiteCommand(s, meta_db);
            meta_dbCommand.ExecuteNonQuery();
            

            try
            {
                

                command = new SQLiteCommand("select * from autompg order by brand desc", m_dbConnection);

                db_size = 0;
                SQLiteDataReader reader = command.ExecuteReader();
                while (reader.Read())
                {
                    db_size++;
                    brand_values.Add((string)reader["brand"]);
                    model_values.Add((string)reader["model"]);
                    type_values.Add((string)reader["type"]);

                }

                //FillIdf_cat(meta_db);

                //FillIdf_num(meta_db);

                /*Console.WriteLine("IDF(8 cylinders): " + IDF("cylinders", 8, m_dbConnection));
                Console.WriteLine("IDF(7 cylinders): " + IDF("cylinders", 7, m_dbConnection));
                Console.WriteLine("IDF(6 cylinders): " + IDF("cylinders",6,m_dbConnection));
                Console.WriteLine("IDF(5 cylinders): " + IDF("cylinders", 5, m_dbConnection));
                Console.WriteLine("IDF(4 cylinders): " + IDF("cylinders", 4, m_dbConnection));
                Console.WriteLine("IDF(3 cylinders): " + IDF("cylinders", 3, m_dbConnection));
                Console.WriteLine("IDF(2 cylinders): " + IDF("cylinders", 2, m_dbConnection));
                Console.WriteLine("IDF(1 cylinders): " + IDF("cylinders", 1, m_dbConnection));
                */
                 

                

            }

            finally
            {
                meta_db.Close();
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

        private static void FillIdf_num(SQLiteConnection meta_db)
        {
            throw new NotImplementedException();
            // omdat er bij numerieke velden een oneindig aantal verschillende waarden zijn, 
            // is het niet mogelijk de hele idf-table al te vullen.
            // een goed alternatief lijkt me om de table te vullen met enkel de waarden die
            // al in de oorspronkelijke table voorkomen.

            // voor iedere kolom
            // voor iedere verschillende waarde
            // bereken de IDF-waarde
            // store de waarde in de db
        }

        private static void FillIdf_cat(SQLiteConnection meta_db)
        {
            

            throw new NotImplementedException();

            // voor iedere kolom
            // voor iedere verschillende waarde 
            // (hardcoded of via sql alle verschillende waarden opvragen)
            // bereken de IDF-waarde
            // store de waarde in de db
        }

        private static double S(string category, string query_value, string db_value, SQLiteConnection db)
        {
            if (query_value.Equals(db_value))
                return IDF(category, query_value, db);
            else
                return 0;
        }

        private static double S(string category, double query_value, double db_value, SQLiteConnection db)
        {
            double h = ComputeH(category, db);
            return
                Math.Pow(Math.E, -0.5 * Math.Pow((db_value - query_value) / h, 2)) *
                IDF(category, query_value, db);
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
            double h = ComputeH(category, db);

            double freq = 0;
            string q = "select " + category + " from autompg";
            SQLiteCommand command = new SQLiteCommand(q, db);
            SQLiteDataReader reader = command.ExecuteReader();

            while (reader.Read())
            {
                freq += Math.Pow(Math.E, -0.5 * Math.Pow((Convert.ToDouble(reader[category]) - value) / h, 2));

            }

            return Math.Log10(db_size / freq);
        }


        private static double ComputeH(string category, SQLiteConnection db)
        {
            return 3.5;

        }


    }
}
