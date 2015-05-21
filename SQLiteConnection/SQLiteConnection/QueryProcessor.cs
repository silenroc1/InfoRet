<<<<<<< HEAD
﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SQLite;

namespace InformationRetrieval
{
    class QueryHandler
    {
        private static SQLiteConnection m_dbConnection = Preprocessor.m_dbConnection;

        private static SQLiteDataReader TupleLookup(int TID){
            SQLiteCommand command = new SQLiteCommand("select * from autompg WHERE id = \'" + TID + "\'", m_dbConnection);
            SQLiteDataReader reader = command.ExecuteReader();
            reader.Read();
            return reader;
        }
     

        private static int IndexLookupGetNextTID(int Lk){
            return 0;
        }
    }
}

