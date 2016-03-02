module ddbserver.sql.parser;

import std.string : empty;
import std.conv : to;

/// token type
enum SqlTokenType : int {
    error,
    eof,
    comment,
    str,
    number,
    floating,

    keyword = 0x100, // reserved range for keywords
    op = 0x200,      // reserved for operators
    ident = 0x400,   // reserved fir idents
}

/// keyword
// should be alphabetically sorted
enum SqlKeyword : int {
    ASC = SqlTokenType.keyword,
    BY,
    CHAR,
    CREATE,
    DESC,
    FROM,
    GROUP,
    IN,
    INDEX,
    IS,
    KEY,
    LIKE,
    NOT,
    NULL,
    ORDER,
    PRIMARY,
    SELECT,
    TABLE,
    UNIQUE,
    VARCHAR,
    WHERE,
}

import std.traits;
string[] enumFieldNames(E)() {
    string[] res;
    foreach(m; __traits(allMembers, E)) {
        res ~= m;
    }
    return res;
}

int[] enumFieldValues(E)() {
    int[] res;
    foreach(m; EnumMembers!E) {
        res ~= m;
    }
    return res;
}

immutable string[] KEYWORD_NAMES = enumFieldNames!SqlKeyword;
immutable int[] KEYWORD_IDS = enumFieldValues!SqlKeyword;

pragma(msg, "KEYWORD_NAMES=", KEYWORD_NAMES);
pragma(msg, "KEYWORD_IDS=", KEYWORD_IDS);

/// compare strings case insensitive; keyword must be already uppercase
int compareKeyword(string s, string keyword) {
    int i = 0;
    for(; i < s.length && i < keyword.length; i++) {
        char ch = s[i];
        char ch2 = keyword[i];
        if (ch >= 'a' && ch <= 'z')
            ch = cast(char)(ch + 'A' - 'a');
        if (ch < ch2)
            return -1;
        if (ch > ch2)
            return 1;
    }
    if (s.length < keyword.length)
        return -1;
    if (s.length > keyword.length)
        return 1;
    return 0;
}

/// returns 0 if not a keyword, SqlKeyword if keyword (s will be replaced with 
int findKeyword(ref string s) {
    int a = 0;
    int b = cast(int)KEYWORD_IDS.length;
    for(;;) {
        int c = (a + b) / 2;
        int res = compareKeyword(s, KEYWORD_NAMES[c]);
        if (!res) {
            s = KEYWORD_NAMES[c];
            return KEYWORD_IDS[c];
        }
        if (a + 1 >= b)
            break;
        if (res < 0)
            b = c;
        else
            a = c + 1;
    }
    return 0;
}


enum SqlOp : int {
    DOT = SqlTokenType.op,   // .
    DIV,
    MOD,
    COMMA, // ,
    PLUS,  // +
    MINUS, // -
    BIT_AND, // &
    AND, // &&
    BIT_OR, // |
    OR, // ||
    BIT_XOR, // ^
    BIT_NOT, // ~
}

/// token
struct SqlToken {
    int id = SqlTokenType.error;
    int pos = 0;
    string str;
    this(int id) {
        this.id = id;
    }
    this(int id, string str) {
        this.id = id;
        this.str = str;
    }
    @property SqlTokenType type() {
        if (id < SqlTokenType.keyword)
            return cast(SqlTokenType)id;
        else if (id >= SqlTokenType.ident)
            return SqlTokenType.ident;
        return cast(SqlTokenType)(id & 0xFF00);
    }
    @property string toString() {
        switch(type) {
            case SqlTokenType.error: return "error";
            case SqlTokenType.keyword: return str;
            case SqlTokenType.op: return "op:" ~ to!string(cast(SqlOp)id);
            case SqlTokenType.ident: return "`" ~ str ~ "` ";
            case SqlTokenType.str: return "\"" ~ str ~ "\"";
            case SqlTokenType.comment: return "comment:\'" ~ str ~ "\'";
            default:
                return str;
        }
    }
    @property bool isError() { return id == SqlTokenType.error; }
    @property bool isEof() { return id == SqlTokenType.eof; }
    @property bool isKeyword() { return (id & 0xFFFFFF00) == SqlTokenType.keyword; }
    @property bool isOp() { return (id & 0xFFFFFF00) == SqlTokenType.op; }
    @property bool isIdent() { return id == SqlTokenType.ident; }
    @property bool isString() { return id == SqlTokenType.str; }
    @property bool isComment() { return id == SqlTokenType.comment; }
    @property bool isNumber() { return id == SqlTokenType.number; }
    @property bool isFloating() { return id == SqlTokenType.floating; }
}

/// skip spaces, return false if end of line reached
bool skipWhiteSpace(ref string s) {
    for (;;) {
        if (s.empty)
            return false;
        char ch = s[0];
        if (ch == ' ' || ch == '\t' || ch == '\r' || ch == '\n') {
            s = s[1 .. $];
        } else {
            break;
        }
    }
    return true;
}

bool isFirstIdentChar(char ch) {
    return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch == '_');
}

bool isMiddleIdentChar(char ch) {
    return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || (ch == '_');
}

import core.sync.mutex;
class StringCache {
    Mutex _mutex;
    int _nextId;
    string[int] _stringById;
    int[string] _idByString;
    this(int initialId) {
        _nextId = initialId;
        _mutex = new Mutex;
    }

    @property Mutex mutex() { 
        return _mutex; 
    }

    int internString(ref string s) {
        if (auto id = s in _idByString) {
            // existing string
            s = _stringById[*id];
            return *id;
        }
        // new string
        s = s.dup;
        int res = _nextId++;
        _stringById[res] = s;
        _idByString[s] = res;
        return res;
    }

    int findString(string s) {
        if (auto id = s in _idByString) {
            return *id;
        }
        return 0;
    }
}

private __gshared StringCache identCache;

__gshared static this() {
    identCache = new StringCache(SqlTokenType.ident);
}

int internString(ref string s) {
    synchronized(identCache.mutex) {
        return identCache.internString(s);
    }
}

SqlToken parseOp(ref string s) {
    SqlToken res;
    char ch = s[0];
    char ch2 = s.length > 1 ? s[1] : 0;
    switch(ch) {
        case ',':
            s = s[1 .. $];
            return SqlToken(SqlOp.COMMA, ",");
        case '.':
            s = s[1 .. $];
            return SqlToken(SqlOp.DOT, ".");
        case '/':
            s = s[1 .. $];
            return SqlToken(SqlOp.DIV, "/");
        case '+':
            s = s[1 .. $];
            return SqlToken(SqlOp.PLUS, "+");
        case '^':
            s = s[1 .. $];
            return SqlToken(SqlOp.BIT_XOR, "^");
        case '~':
            s = s[1 .. $];
            return SqlToken(SqlOp.BIT_NOT, "~");
        case '&':
            if (ch2 == '&') {
                s = s[2 .. $];
                return SqlToken(SqlOp.AND, "&&");
            }
            s = s[1 .. $];
            return SqlToken(SqlOp.BIT_AND, "&");
        case '|':
            if (ch2 == '|') {
                s = s[2 .. $];
                return SqlToken(SqlOp.OR, "||");
            }
            s = s[1 .. $];
            return SqlToken(SqlOp.BIT_OR, "|");
        case '-':
            s = s[1 .. $];
            return SqlToken(SqlOp.MINUS, "-");
        default:
            break;
    }
    return res;
}

SqlToken parseString(ref string s) {
    static char[] buf;
    SqlToken res;
    char quoteChar = s[0];
    int i = 1;
    bool foundClosingQuote = false;
    if (buf.length < 1024)
        buf.length = 1024;
    buf.length = 0;
    int pos = 0;
    for (; i < s.length; i++) {
        char ch = s[i];
        char ch2 = i + 1 < s.length ? s[i + 1] : 0;
        if (ch == quoteChar && ch2 == quoteChar) { // double '' or ""
            buf[pos++] = ch;
            i++;
        } else if (ch == quoteChar) {
            foundClosingQuote = true;
            break;
        } else if (ch == '\\') {
            // escape sequence
            if (pos >= buf.length)
                buf.length = pos + 1024;
            switch(ch2) {
                case '{':
                case '}':
                case '&':
                case '%':
                case '_':
                    buf[pos++] = ch2;
                    break;
                case 'b':
                    buf[pos++] = '\b';
                    break;
                case 'r':
                    buf[pos++] = '\r';
                    break;
                case '\'':
                    buf[pos++] = '\'';
                    break;
                case '\"':
                    buf[pos++] = '\"';
                    break;
                case 'n':
                    buf[pos++] = '\n';
                    break;
                case 't':
                    buf[pos++] = '\t';
                    break;
                case 'Z':
                    buf[pos++] = 26;
                    break;
                case '0':
                    buf[pos++] = 0;
                    break;
                case '\\':
                    buf[pos++] = '\\';
                    break;
                default:
                    return SqlToken(SqlTokenType.error, "Invalid escape sequence in string token");
            }
            i++;
        } else {
            if (pos >= buf.length)
                buf.length = pos + 1024;
            buf[pos++] = ch;
        }
    }
    if (foundClosingQuote) {
        s = s[i + 1 .. $];
        return SqlToken(SqlTokenType.str, buf[0..pos].dup);
    }
    return SqlToken(SqlTokenType.error, "Invalid string token");
}

SqlToken parseNumber(ref string s) {
    static char[] buf;
    SqlToken res;
    int i = 1;
    if (buf.length < 1024)
        buf.length = 1024;
    buf.length = 0;
    int pos = 0;
    for (; i < s.length; i++) {
        char ch = s[i];
        if (ch >= '0' && ch <= '9') {
            if (pos >= buf.length)
                buf.length = pos + 1024;
            buf[pos++] = ch;
        } else {
            break;
        }
    }
    s = s[i .. $];
    return SqlToken(SqlTokenType.number, buf[0..pos].dup);
}

SqlToken parseComment(ref string s) {
    char commentChar = s[0];
    SqlToken res;
    int start = commentChar == '#' ? 1 : 2;
    int i = start;
    for (; i < s.length; i++) {
        char ch = s[i];
        char ch2 = i + 1 < s.length ? s[i + 1] : 0;
        if (commentChar != '/' && (ch == '\r' || ch == '\n')) {
            res = SqlToken(SqlTokenType.comment, s[0 .. i]);
            s = s[i .. $];
            skipWhiteSpace(s);
            return res;
        }
        if (commentChar == '/' && ch == '*' && ch2 == '/') {
            i += 2;
            res = SqlToken(SqlTokenType.comment, s[0 .. i]);
            s = s[i .. $];
            skipWhiteSpace(s);
            return res;
        }
    }
    if (commentChar != '/') {
        res = SqlToken(SqlTokenType.comment, s[0 .. i]);
        s = null;
        return res;
    }
    return SqlToken(SqlTokenType.error, "Unclosed comment");
}

SqlToken parseToken(ref string s) {
    if (!skipWhiteSpace(s))
        return SqlToken(SqlTokenType.eof);
    char ch = s[0];
    char ch2 = s.length > 1 ? s[1] : 0;
    if (ch == '#') // # comment
        return parseComment(s);
    if (ch == '/' && ch2 == '*') // /* comment
        return parseComment(s);
    if (ch == '-' && ch2 == '-') // -- comment
        return parseComment(s);
    if (ch.isFirstIdentChar) {
        int i = 1;
        for (; i < s.length; i++) {
            if (!isMiddleIdentChar(s[i]))
                break;
        }
        string identString = s[0 .. i];
        s = s[i .. $];
        int id = findKeyword(identString);
        if (id) // found keyword
            return SqlToken(id, identString);
        id = identCache.internString(identString);
        return SqlToken(id, identString);
    }
    if (ch >= '0' && ch <='9')
        return parseNumber(s);
    if (ch == '`') {
        // quoted ident in backquotes like `COLUMN NAME`
        int i = 1;
        bool foundClosingQuote = false;
        for (; i < s.length; i++) {
            if (s[i] == '`') {
                foundClosingQuote = true;
                break;
            }
        }
        if (!foundClosingQuote)
            return SqlToken(SqlTokenType.error, "unclosed quoted identifier");
        string identString = s[1 .. i];
        s = s[i + 1 .. $];
        int id = identCache.internString(identString);
        return SqlToken(id, identString);
    }
    if (ch == '\'' || ch == '\"')
        return parseString(s);
    // check for operator
    SqlToken res = parseOp(s);
    if (res.id)
        return res;

    return SqlToken(SqlTokenType.error);
}

class ParsedSql {
    string sql;
    SqlToken[] tokens;
    int errorCode;
    string errorMsg;
    int errorPos;
    this(string sql) {
        this.sql = sql;
    }
    bool parse() {
        string s = sql;
        synchronized(identCache.mutex) {
            for (;;) {
                if (!skipWhiteSpace(s))
                    break;
                int pos = cast(int)(sql.length - s.length);
                SqlToken tok = parseToken(s);
                if (tok.id == SqlTokenType.eof)
                    break;
                if (!tok.id) {
                    // handle tokenize error
                    errorCode = 1;
                    errorMsg = tok.str;
                    errorPos = pos;
                    break;
                }
                tok.pos = pos;
                tokens ~= tok;
            }
        }
        return errorCode == 0;
    }
}

ParsedSql parseSQL(string sql) {
    ParsedSql res = new ParsedSql(sql);
    res.parse();
    return res;
}
