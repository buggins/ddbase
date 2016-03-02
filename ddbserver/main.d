import std.stdio;
import ddbserver.core.logger;
import ddbserver.sql.parser;

int main(string[] argv)
{
    Log.setFileLogger(new std.stdio.File("server.log", "w"));
    Log.setLogLevel(LogLevel.Trace);
    Log.i("Starting server");

    ParsedSql sql = parseSQL("SELECT id, `name`, `some Column`, \n'some string\nnewline' /* multiline \n comment */ From campaign #singleline comment \nwhere name like '%bla%' ORDER BY id DESC -- one more single line comment");
    Log.d("Parsed sql '", sql.sql, "' : ", sql.tokens);

    writeln("Hello D-World!");
    return 0;
}
