using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SQLite;

namespace SQLite
{
    class SQLiteConnecter
    {

        static void Main(string[] args)
        {
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

            Console.Read();
        }
    }
}
